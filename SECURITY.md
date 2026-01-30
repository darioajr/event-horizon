# Política de Segurança

## Versões Suportadas

| Versão | Suportada          |
| ------ | ------------------ |
| 0.1.x  | :white_check_mark: |

## Reportando uma Vulnerabilidade

Se você descobrir uma vulnerabilidade de segurança no Event Horizon, por favor reporte de forma responsável.

### Como Reportar

1. **NÃO** abra uma issue pública para vulnerabilidades de segurança
2. Envie um email para: **security@seu-dominio.com**
3. Ou use o [GitHub Security Advisories](../../security/advisories/new)

### O que incluir no relatório

- Descrição detalhada da vulnerabilidade
- Passos para reproduzir o problema
- Possível impacto da vulnerabilidade
- Sugestões de correção (se houver)

### O que esperar

- **Confirmação**: Responderemos em até 48 horas confirmando o recebimento
- **Avaliação**: Avaliaremos a vulnerabilidade em até 7 dias
- **Correção**: Vulnerabilidades críticas serão corrigidas em até 30 dias
- **Divulgação**: Coordenaremos a divulgação pública após a correção

### Reconhecimento

Agradecemos a todos que reportam vulnerabilidades de forma responsável. Contribuidores de segurança serão reconhecidos no CHANGELOG (com permissão).

## Práticas de Segurança

### No Desenvolvimento

- Código revisado por pares antes do merge
- Análise estática de código no CI
- Testes de segurança automatizados
- Dependências atualizadas regularmente

### No Protocolo

- Validação rigorosa de entrada
- Limites de tamanho para requisições
- Timeout para conexões
- Rate limiting (planejado)

### Na Implantação

- Executar com privilégios mínimos
- Usar firewall para limitar acesso
- Monitorar logs de acesso
- Manter versão atualizada

## Vulnerabilidades Conhecidas

Nenhuma vulnerabilidade conhecida no momento.

## Histórico de Segurança

| Data | Versão | Descrição | CVE |
|------|--------|-----------|-----|
| - | - | Nenhum registro | - |
