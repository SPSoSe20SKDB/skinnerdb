SELECT AVG(CAST((CAST(wins_4.xwin AS double) / 2 ) AS double)) AS avgcalculation_2642768498540961792ok,   CAST(wins_4.nfpsx AS LONG) AS nfpsx FROM wins_4 WHERE ((wins_4.ttrk3 NOT IN ('MD', 'RET', 'PLN', 'ONE', 'BCF', 'WRD', 'HST', 'HPO', 'PRV', 'ASD', 'CLS', 'FAR', 'TIL', 'GPR', 'ELK', 'ARP', 'SWF', 'EMD', 'CAS', 'CBY', 'SDY', 'ZIA', 'CWF', 'RIL', 'RP', 'BKF', 'FON', 'ALB', 'FER', 'SRP', 'RUI', 'DEP', 'ELY', 'GF', 'EMT', 'GIL', 'MED', 'ABT', 'PHA', 'BOI', 'OTC', 'UN', 'LNN', 'SUD', 'LBG', 'WYO', 'SON', 'MIL', 'FTP', 'GRP', 'FMT', 'CPW')) AND (wins_4.ttrk3 >= 'AP') AND (wins_4.ttrk3 <= 'WO')) GROUP BY wins_4.nfpsx,   nfpsx;
