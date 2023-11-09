

WITH tickets AS (
    SELECT
        *
    FROM
        `agile-scheme-394814`.`dbt_dev`.`base_caresoft__tickets`
),
ticket_details AS (
    SELECT
        *
    FROM
        `agile-scheme-394814`.`dbt_dev`.`base_caresoft__ticket_details`
)
SELECT
    t.assignee,
    t.ccs,
    t.created_at,
    t.custom_fields,
    t.follows,
    t.is_overdue,
    t.requester,
    t.requester_id,
    t.service_id,
    t.tags,
    t.ticket_id,
    t.ticket_no,
    t.ticket_priority,
    t.ticket_source,
    t.ticket_source_end_status,
    t.ticket_status,
    t.ticket_subject,
    t.updated_at,
    t.assignee_id,
    td.account_id,
    td.automessage_id,
    td.campaign_id,
    td.comments,
    td.current_agent,
    td.duedate,
    td.feedback_status,
    td.group_id,
    td.incident_id,
    td.qa_agent,
    td.qa_script_id,
    td.satisfaction,
    td.satisfaction_at,
    td.satisfaction_content,
    td.satisfaction_send,
    td.sla,
    td.sla_id,
    td.ticket_source_detail_id,
FROM
    tickets t
    LEFT JOIN ticket_details td
    ON t.ticket_id = td.ticket_id