SELECT (CASE WHEN (CAST(wins_3.nfin AS LONG) = 1) THEN 'Yes' ELSE 'No' END) AS calculation_1136595897793773568 FROM wins_3 WHERE (wins_3.ttrk3 NOT IN ('MD', 'RET', 'PLN', 'ONE', 'BCF', 'WRD', 'HST', 'HPO', 'PRV', 'ASD', 'CLS', 'FAR', 'TIL', 'GPR', 'ELK', 'ARP', 'SWF', 'EMD', 'CAS', 'CBY', 'SDY', 'ZIA', 'CWF', 'RIL', 'RP', 'BKF', 'FON', 'ALB', 'FER', 'SRP', 'RUI', 'DEP', 'ELY', 'GF', 'EMT', 'GIL', 'MED', 'ABT', 'PHA', 'BOI', 'OTC', 'UN', 'LNN', 'SUD', 'LBG', 'WYO', 'SON', 'MIL', 'FTP', 'GRP', 'FMT', 'CPW')) GROUP BY calculation_1136595897793773568 ORDER BY calculation_1136595897793773568 ASC ;
