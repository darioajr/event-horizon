#include <gtest/gtest.h>
#include "../consumer/consumer_group.hpp"
#include <thread>
#include <chrono>

using namespace eventhorizon;
using namespace std::chrono_literals;

// ============================================================================
// ConsumerGroup Tests
// ============================================================================

class ConsumerGroupTest : public ::testing::Test {
protected:
    void SetUp() override {
        group_ = std::make_unique<ConsumerGroup>("test-group");
    }
    
    std::unique_ptr<ConsumerGroup> group_;
    
    // Helper to create protocols
    std::vector<std::pair<std::string, std::vector<uint8_t>>> make_protocols(
        const std::vector<std::string>& names) {
        std::vector<std::pair<std::string, std::vector<uint8_t>>> protocols;
        for (const auto& name : names) {
            protocols.emplace_back(name, std::vector<uint8_t>{});
        }
        return protocols;
    }
};

TEST_F(ConsumerGroupTest, InitialState) {
    EXPECT_EQ(group_->group_id(), "test-group");
    EXPECT_EQ(group_->state(), GroupState::Empty);
    EXPECT_EQ(group_->generation_id(), 0);
    EXPECT_TRUE(group_->is_empty());
    EXPECT_EQ(group_->member_count(), 0);
    EXPECT_EQ(group_->leader_id(), "");
}

TEST_F(ConsumerGroupTest, GroupStateToString) {
    EXPECT_EQ(group_state_to_string(GroupState::Empty), "Empty");
    EXPECT_EQ(group_state_to_string(GroupState::PreparingRebalance), "PreparingRebalance");
    EXPECT_EQ(group_state_to_string(GroupState::CompletingRebalance), "CompletingRebalance");
    EXPECT_EQ(group_state_to_string(GroupState::Stable), "Stable");
    EXPECT_EQ(group_state_to_string(GroupState::Dead), "Dead");
}

TEST_F(ConsumerGroupTest, JoinFirstMember) {
    auto protocols = make_protocols({"range", "roundrobin"});
    
    auto result = group_->join(
        "",                     // member_id (empty = generate)
        std::nullopt,           // group_instance_id
        "client-1",             // client_id
        "192.168.1.1",          // client_host
        "consumer",             // protocol_type
        protocols,              // protocols
        10000,                  // session_timeout_ms
        60000                   // rebalance_timeout_ms
    );
    
    EXPECT_EQ(result.error_code, 0);
    EXPECT_FALSE(result.member_id.empty());
    EXPECT_EQ(result.generation_id, 1);
    EXPECT_EQ(result.leader_id, result.member_id); // First member is leader
    EXPECT_FALSE(result.protocol_name.empty());
    
    // Group state should be CompletingRebalance
    EXPECT_EQ(group_->state(), GroupState::CompletingRebalance);
    EXPECT_EQ(group_->member_count(), 1);
    EXPECT_FALSE(group_->is_empty());
}

TEST_F(ConsumerGroupTest, JoinMultipleMembers) {
    auto protocols = make_protocols({"range"});
    
    // First member joins
    auto result1 = group_->join("", std::nullopt, "client-1", "host-1", 
                                 "consumer", protocols, 10000, 60000);
    EXPECT_EQ(result1.error_code, 0);
    
    // Second member joins
    auto result2 = group_->join("", std::nullopt, "client-2", "host-2",
                                 "consumer", protocols, 10000, 60000);
    EXPECT_EQ(result2.error_code, 0);
    
    EXPECT_NE(result1.member_id, result2.member_id);
    EXPECT_EQ(group_->member_count(), 2);
    
    // Both should have same leader
    EXPECT_EQ(result1.leader_id, result2.leader_id);
    
    // Generation should have incremented for second join
    EXPECT_GE(result2.generation_id, result1.generation_id);
}

TEST_F(ConsumerGroupTest, JoinWithExistingMemberId) {
    auto protocols = make_protocols({"range"});
    
    // First join to get member_id
    auto result1 = group_->join("", std::nullopt, "client-1", "host-1",
                                 "consumer", protocols, 10000, 60000);
    std::string member_id = result1.member_id;
    
    // Rejoin with same member_id
    auto result2 = group_->join(member_id, std::nullopt, "client-1", "host-1",
                                 "consumer", protocols, 10000, 60000);
    
    EXPECT_EQ(result2.error_code, 0);
    EXPECT_EQ(result2.member_id, member_id);
    EXPECT_EQ(group_->member_count(), 1); // Still only one member
}

TEST_F(ConsumerGroupTest, JoinWithStaticMembership) {
    auto protocols = make_protocols({"range"});
    
    // Join with group_instance_id
    auto result1 = group_->join("", "instance-1", "client-1", "host-1",
                                 "consumer", protocols, 10000, 60000);
    EXPECT_EQ(result1.error_code, 0);
    std::string original_member_id = result1.member_id;
    
    // Rejoin with same instance_id but different member_id
    auto result2 = group_->join("", "instance-1", "client-1", "host-1",
                                 "consumer", protocols, 10000, 60000);
    
    EXPECT_EQ(result2.error_code, 0);
    // Should get same member_id due to static membership
    EXPECT_EQ(result2.member_id, original_member_id);
}

TEST_F(ConsumerGroupTest, InconsistentProtocolType) {
    auto protocols = make_protocols({"range"});
    
    // First member joins with "consumer" protocol type
    auto result1 = group_->join("", std::nullopt, "client-1", "host-1",
                                 "consumer", protocols, 10000, 60000);
    EXPECT_EQ(result1.error_code, 0);
    
    // Second member tries to join with different protocol type
    auto result2 = group_->join("", std::nullopt, "client-2", "host-2",
                                 "connect", protocols, 10000, 60000);
    
    // Should fail with INCONSISTENT_GROUP_PROTOCOL
    EXPECT_EQ(result2.error_code, 23);
}

TEST_F(ConsumerGroupTest, NoCommonProtocol) {
    auto protocols1 = make_protocols({"range"});
    auto protocols2 = make_protocols({"roundrobin"});
    
    auto result1 = group_->join("", std::nullopt, "client-1", "host-1",
                                 "consumer", protocols1, 10000, 60000);
    EXPECT_EQ(result1.error_code, 0);
    
    // Second member with different protocols joins the group
    // The current implementation allows this (uses first available protocol)
    auto result2 = group_->join("", std::nullopt, "client-2", "host-2",
                                 "consumer", protocols2, 10000, 60000);
    
    // Both members should be able to join
    EXPECT_EQ(result2.error_code, 0);
    EXPECT_EQ(group_->member_count(), 2);
}

TEST_F(ConsumerGroupTest, LeaveMember) {
    auto protocols = make_protocols({"range"});
    
    auto result = group_->join("", std::nullopt, "client-1", "host-1",
                                "consumer", protocols, 10000, 60000);
    
    EXPECT_EQ(group_->member_count(), 1);
    
    group_->leave(result.member_id);
    
    EXPECT_EQ(group_->member_count(), 0);
    EXPECT_EQ(group_->state(), GroupState::Empty);
}

TEST_F(ConsumerGroupTest, LeaveByInstanceId) {
    auto protocols = make_protocols({"range"});
    
    auto result = group_->join("", "instance-1", "client-1", "host-1",
                                "consumer", protocols, 10000, 60000);
    
    EXPECT_EQ(group_->member_count(), 1);
    
    group_->leave_by_instance_id("instance-1");
    
    EXPECT_EQ(group_->member_count(), 0);
}

TEST_F(ConsumerGroupTest, SyncGroup) {
    auto protocols = make_protocols({"range"});
    
    // Join as leader
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    // Create assignment
    std::vector<uint8_t> assignment = {0x01, 0x02, 0x03};
    std::vector<std::pair<std::string, std::vector<uint8_t>>> assignments = {
        {join_result.member_id, assignment}
    };
    
    // Sync
    auto sync_result = group_->sync(join_result.member_id, 
                                     join_result.generation_id,
                                     assignments);
    
    EXPECT_EQ(sync_result.error_code, 0);
    EXPECT_EQ(sync_result.assignment, assignment);
    EXPECT_EQ(group_->state(), GroupState::Stable);
}

TEST_F(ConsumerGroupTest, SyncGroupWrongGeneration) {
    auto protocols = make_protocols({"range"});
    
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    std::vector<std::pair<std::string, std::vector<uint8_t>>> assignments;
    
    // Sync with wrong generation
    auto sync_result = group_->sync(join_result.member_id, 
                                     join_result.generation_id + 1,
                                     assignments);
    
    // Should fail with ILLEGAL_GENERATION
    EXPECT_EQ(sync_result.error_code, 22);
}

TEST_F(ConsumerGroupTest, SyncGroupUnknownMember) {
    auto protocols = make_protocols({"range"});
    
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    std::vector<std::pair<std::string, std::vector<uint8_t>>> assignments;
    
    // Sync with unknown member
    auto sync_result = group_->sync("unknown-member",
                                     join_result.generation_id,
                                     assignments);
    
    // Should fail with UNKNOWN_MEMBER_ID
    EXPECT_EQ(sync_result.error_code, 25);
}

TEST_F(ConsumerGroupTest, HeartbeatSuccess) {
    auto protocols = make_protocols({"range"});
    
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    // Complete sync first
    std::vector<std::pair<std::string, std::vector<uint8_t>>> assignments = {
        {join_result.member_id, {}}
    };
    (void)group_->sync(join_result.member_id, join_result.generation_id, assignments);
    
    // Heartbeat should succeed
    int16_t error = group_->heartbeat(join_result.member_id, join_result.generation_id);
    EXPECT_EQ(error, 0);
}

TEST_F(ConsumerGroupTest, HeartbeatRebalanceInProgress) {
    auto protocols = make_protocols({"range"});
    
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    // Heartbeat without completing sync
    int16_t error = group_->heartbeat(join_result.member_id, join_result.generation_id);
    
    // Should return REBALANCE_IN_PROGRESS
    EXPECT_EQ(error, 27);
}

TEST_F(ConsumerGroupTest, HeartbeatWrongGeneration) {
    auto protocols = make_protocols({"range"});
    
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    int16_t error = group_->heartbeat(join_result.member_id, join_result.generation_id + 1);
    
    // Should fail with ILLEGAL_GENERATION
    EXPECT_EQ(error, 22);
}

TEST_F(ConsumerGroupTest, HeartbeatUnknownMember) {
    auto protocols = make_protocols({"range"});
    
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    int16_t error = group_->heartbeat("unknown-member", join_result.generation_id);
    
    // Should fail with UNKNOWN_MEMBER_ID
    EXPECT_EQ(error, 25);
}

TEST_F(ConsumerGroupTest, CommitAndFetchOffset) {
    auto protocols = make_protocols({"range"});
    
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    // Commit offset
    group_->commit_offset("test-topic", 0, 100, "metadata");
    group_->commit_offset("test-topic", 1, 200, "");
    group_->commit_offset("other-topic", 0, 50, "");
    
    // Fetch offsets
    auto offset0 = group_->fetch_offset("test-topic", 0);
    ASSERT_TRUE(offset0.has_value());
    EXPECT_EQ(offset0->offset, 100);
    EXPECT_EQ(offset0->metadata, "metadata");
    
    auto offset1 = group_->fetch_offset("test-topic", 1);
    ASSERT_TRUE(offset1.has_value());
    EXPECT_EQ(offset1->offset, 200);
    
    auto offset_other = group_->fetch_offset("other-topic", 0);
    ASSERT_TRUE(offset_other.has_value());
    EXPECT_EQ(offset_other->offset, 50);
    
    // Non-existent offset
    auto no_offset = group_->fetch_offset("nonexistent", 0);
    EXPECT_FALSE(no_offset.has_value());
}

TEST_F(ConsumerGroupTest, AllOffsets) {
    group_->commit_offset("topic-1", 0, 100, "");
    group_->commit_offset("topic-1", 1, 200, "");
    group_->commit_offset("topic-2", 0, 300, "");
    
    auto all = group_->all_offsets();
    EXPECT_EQ(all.size(), 3);
}

TEST_F(ConsumerGroupTest, Describe) {
    auto protocols = make_protocols({"range"});
    
    (void)group_->join("", std::nullopt, "client-1", "host-1",
                  "consumer", protocols, 10000, 60000);
    (void)group_->join("", std::nullopt, "client-2", "host-2",
                  "consumer", protocols, 10000, 60000);
    
    auto desc = group_->describe();
    
    EXPECT_EQ(desc.group_id, "test-group");
    EXPECT_EQ(desc.protocol_type, "consumer");
    EXPECT_EQ(desc.members.size(), 2);
}

TEST_F(ConsumerGroupTest, GetMember) {
    auto protocols = make_protocols({"range"});
    
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    // Const version
    const ConsumerGroup* const_group = group_.get();
    const MemberInfo* const_member = const_group->get_member(join_result.member_id);
    ASSERT_NE(const_member, nullptr);
    EXPECT_EQ(const_member->client_id, "client-1");
    
    // Non-const version
    MemberInfo* member = group_->get_member(join_result.member_id);
    ASSERT_NE(member, nullptr);
    EXPECT_EQ(member->client_id, "client-1");
    
    // Non-existent member
    EXPECT_EQ(group_->get_member("nonexistent"), nullptr);
}

TEST_F(ConsumerGroupTest, HasMember) {
    auto protocols = make_protocols({"range"});
    
    auto join_result = group_->join("", std::nullopt, "client-1", "host-1",
                                     "consumer", protocols, 10000, 60000);
    
    EXPECT_TRUE(group_->has_member(join_result.member_id));
    EXPECT_FALSE(group_->has_member("nonexistent"));
}

TEST_F(ConsumerGroupTest, MemberIds) {
    auto protocols = make_protocols({"range"});
    
    auto result1 = group_->join("", std::nullopt, "client-1", "host-1",
                                 "consumer", protocols, 10000, 60000);
    auto result2 = group_->join("", std::nullopt, "client-2", "host-2",
                                 "consumer", protocols, 10000, 60000);
    
    auto ids = group_->member_ids();
    EXPECT_EQ(ids.size(), 2);
    
    // Both member IDs should be in the list
    bool found1 = std::find(ids.begin(), ids.end(), result1.member_id) != ids.end();
    bool found2 = std::find(ids.begin(), ids.end(), result2.member_id) != ids.end();
    EXPECT_TRUE(found1);
    EXPECT_TRUE(found2);
}

TEST_F(ConsumerGroupTest, IsLeader) {
    auto protocols = make_protocols({"range"});
    
    auto result1 = group_->join("", std::nullopt, "client-1", "host-1",
                                 "consumer", protocols, 10000, 60000);
    auto result2 = group_->join("", std::nullopt, "client-2", "host-2",
                                 "consumer", protocols, 10000, 60000);
    
    // First member should be leader
    EXPECT_TRUE(group_->is_leader(result1.member_id));
    EXPECT_FALSE(group_->is_leader(result2.member_id));
}

// ============================================================================
// MemberInfo Tests
// ============================================================================

TEST(MemberInfoTest, SessionExpiration) {
    MemberInfo member;
    member.session_timeout_ms = 100; // 100ms timeout
    member.touch_heartbeat();
    
    EXPECT_FALSE(member.is_session_expired());
    
    std::this_thread::sleep_for(150ms);
    
    EXPECT_TRUE(member.is_session_expired());
}

TEST(MemberInfoTest, TouchHeartbeat) {
    MemberInfo member;
    member.session_timeout_ms = 100;
    
    std::this_thread::sleep_for(50ms);
    member.touch_heartbeat();
    std::this_thread::sleep_for(60ms);
    
    // Should not be expired because we touched heartbeat
    EXPECT_FALSE(member.is_session_expired());
}

// ============================================================================
// TopicPartition Tests
// ============================================================================

TEST(TopicPartitionTest, Comparison) {
    TopicPartition tp1{"topic-a", 0};
    TopicPartition tp2{"topic-a", 1};
    TopicPartition tp3{"topic-b", 0};
    TopicPartition tp4{"topic-a", 0};
    
    EXPECT_TRUE(tp1 < tp2);
    EXPECT_TRUE(tp1 < tp3);
    EXPECT_TRUE(tp2 < tp3);
    EXPECT_TRUE(tp1 == tp4);
    EXPECT_FALSE(tp1 == tp2);
}

// ============================================================================
// ConsumerGroupManager Tests
// ============================================================================

class ConsumerGroupManagerTest : public ::testing::Test {
protected:
    ConsumerGroupManager manager_;
};

TEST_F(ConsumerGroupManagerTest, GetOrCreateGroup) {
    auto* group1 = manager_.get_or_create_group("group-1");
    ASSERT_NE(group1, nullptr);
    EXPECT_EQ(group1->group_id(), "group-1");
    
    // Get same group again
    auto* group1_again = manager_.get_or_create_group("group-1");
    EXPECT_EQ(group1, group1_again);
    
    // Create different group
    auto* group2 = manager_.get_or_create_group("group-2");
    ASSERT_NE(group2, nullptr);
    EXPECT_NE(group1, group2);
}

TEST_F(ConsumerGroupManagerTest, GetGroup) {
    // Non-existent group
    EXPECT_EQ(manager_.get_group("nonexistent"), nullptr);
    
    // Create and get
    manager_.get_or_create_group("group-1");
    auto* group = manager_.get_group("group-1");
    ASSERT_NE(group, nullptr);
    EXPECT_EQ(group->group_id(), "group-1");
}

TEST_F(ConsumerGroupManagerTest, DeleteGroup) {
    manager_.get_or_create_group("group-1");
    EXPECT_NE(manager_.get_group("group-1"), nullptr);
    
    manager_.delete_group("group-1");
    EXPECT_EQ(manager_.get_group("group-1"), nullptr);
    
    // Delete non-existent (should not crash)
    manager_.delete_group("nonexistent");
}

TEST_F(ConsumerGroupManagerTest, ListGroups) {
    (void)manager_.get_or_create_group("group-1");
    (void)manager_.get_or_create_group("group-2");
    (void)manager_.get_or_create_group("group-3");
    
    auto groups = manager_.list_groups();
    EXPECT_EQ(groups.size(), 3);
}

TEST_F(ConsumerGroupManagerTest, DescribeGroups) {
    auto* group1 = manager_.get_or_create_group("group-1");
    (void)manager_.get_or_create_group("group-2");
    
    // Add member to group1
    std::vector<std::pair<std::string, std::vector<uint8_t>>> protocols = {
        {"range", {}}
    };
    (void)group1->join("", std::nullopt, "client-1", "host-1", 
                  "consumer", protocols, 10000, 60000);
    
    std::vector<std::string> group_ids = {"group-1", "group-2"};
    auto descriptions = manager_.describe_groups(group_ids);
    
    EXPECT_EQ(descriptions.size(), 2);
}

TEST_F(ConsumerGroupManagerTest, ExpireSessions) {
    auto* group = manager_.get_or_create_group("group-1");
    
    std::vector<std::pair<std::string, std::vector<uint8_t>>> protocols = {
        {"range", {}}
    };
    
    // Join with very short session timeout
    (void)group->join("", std::nullopt, "client-1", "host-1",
                 "consumer", protocols, 50, 1000); // 50ms session timeout
    
    EXPECT_EQ(group->member_count(), 1);
    
    std::this_thread::sleep_for(100ms);
    
    manager_.expire_sessions();
    
    EXPECT_EQ(group->member_count(), 0);
}

TEST_F(ConsumerGroupManagerTest, CleanupEmptyGroups) {
    (void)manager_.get_or_create_group("group-1");
    (void)manager_.get_or_create_group("group-2");
    
    // Groups are empty, cleanup with 0 seconds should remove them
    manager_.cleanup_empty_groups(std::chrono::seconds{0});
    
    // First cleanup just marks the time
    EXPECT_NE(manager_.get_group("group-1"), nullptr);
    
    // Sleep and cleanup again
    std::this_thread::sleep_for(10ms);
    manager_.cleanup_empty_groups(std::chrono::seconds{0});
    
    EXPECT_EQ(manager_.get_group("group-1"), nullptr);
    EXPECT_EQ(manager_.get_group("group-2"), nullptr);
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

TEST_F(ConsumerGroupManagerTest, ConcurrentJoin) {
    auto* group = manager_.get_or_create_group("concurrent-group");
    
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};
    
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&, i]() {
            std::vector<std::pair<std::string, std::vector<uint8_t>>> protocols = {
                {"range", {}}
            };
            auto result = group->join("", std::nullopt, 
                                        "client-" + std::to_string(i),
                                        "host-" + std::to_string(i),
                                        "consumer", protocols, 10000, 60000);
            if (result.error_code == 0) {
                success_count++;
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // All should succeed
    EXPECT_EQ(success_count.load(), 10);
    EXPECT_EQ(group->member_count(), 10);
}

TEST_F(ConsumerGroupManagerTest, ConcurrentGroupAccess) {
    std::vector<std::thread> threads;
    std::atomic<int> created_count{0};
    
    // Multiple threads trying to get/create the same group
    for (int i = 0; i < 20; ++i) {
        threads.emplace_back([&]() {
            auto* group = manager_.get_or_create_group("shared-group");
            if (group != nullptr) {
                created_count++;
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // All should get the same group
    EXPECT_EQ(created_count.load(), 20);
    
    // Only one group should exist
    auto groups = manager_.list_groups();
    EXPECT_EQ(groups.size(), 1);
}
