COPY {{table}} ({{columns|join(', ')}})
FROM STDIN WITH CSV; 
