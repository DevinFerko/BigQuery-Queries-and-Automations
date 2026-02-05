SELECT DISTINCT
  SUM(chats_count)
FROM
  `tech-analytics-data.improvado.livechat_agents`
WHERE date_yyyymmdd >= '20251201'
  AND date_yyyymmdd <= '20260104'
  AND agent_id IN (
      'liam.mcgee@beyondretail.co.uk', 
      'lottie.rogers@beyondretail.co.uk',
      'robert.price@beyondretail.co.uk',
      'stuart.plant@beyondretail.co.uk',
      'scott.thompson@beyondretail.co.uk',
      'susan.oliver@beyondretail.co.uk')