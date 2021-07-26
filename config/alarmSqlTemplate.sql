SELECT 
id,
eventid,
substring(replace (source,'prov:default:/tag:', ""),1, instr(replace (source,'prov:default:/tag:', ""),":/alm:")-1) as tagpath,
displaypath, 
priority, 
eventtype, 
eventtime,
(select strvalue from alarm_event_data where id = alarm_events.id and propname = 'name') as name, 
substring((select strvalue from alarm_event_data where id = alarm_events.id and propname = 'ackUser'),
instr((select strvalue from alarm_event_data where id = alarm_events.id and propname = 'ackUser'),"usr:")+4) as ackby, 
(select floatvalue from alarm_event_data where id = alarm_events.id and propname = 'eventValue') as value
FROM alarm_events where eventtime >= '%s' and eventtime < '%s'
