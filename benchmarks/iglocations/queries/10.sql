SELECT iglocations2_2.calculation_1750724145742463 AS calculation_1750724145742463,   iglocations2_2.calculation_9330724145728972 AS calculation_9330724145728972,   iglocations2_2.state AS state,   SUM(CAST(iglocations2_2.like_count AS LONG)) AS sumlike_countok FROM iglocations2_2 WHERE ((iglocations2_2.state IN ('Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas')) AND (iglocations2_2.state_copy = 'Alaska')) GROUP BY iglocations2_2.calculation_1750724145742463,   iglocations2_2.calculation_9330724145728972, iglocations2_2.state;
