SELECT iglocations2_1.calculation_8090724143600502 AS calculation_8090724143600502,   iglocations2_1.city AS city,   iglocations2_1.state AS state,   SUM(CAST(iglocations2_1.number_of_records AS LONG)) AS sumnumber_of_recordsok FROM iglocations2_1 WHERE ((iglocations2_1.calculation_8090724143600502 = 'Drunk') AND (iglocations2_1.city <> 'Unalaska') AND (iglocations2_1.state IN ('Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas')) AND (iglocations2_1.calculation_4370724142342227 = 'Beach')) GROUP BY iglocations2_1.calculation_8090724143600502,   iglocations2_1.city, iglocations2_1.state;
