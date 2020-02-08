SELECT iglocations2_1.media_type AS media_type,   iglocations2_1.state AS state,   SUM(CAST(iglocations2_1.number_of_records AS LONG)) AS sumnumber_of_recordsok FROM iglocations2_1 WHERE (iglocations2_1.state IN ('Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas')) GROUP BY iglocations2_1.media_type,   iglocations2_1.state;
