SELECT realestate1_2.county AS county,   SUM(CAST(realestate1_2.number_of_records AS LONG)) AS sumnumber_of_recordsok FROM realestate1_2 WHERE ((CAST(realestate1_2.date_of_transfer as DATE) >= date '2005-01-01') AND (CAST(realestate1_2.date_of_transfer as DATE) <= date '2015-03-31') AND (CAST(realestate1_2.calculation_5480628224156393 as DATE) >= date '2005-01-01') AND (CAST(realestate1_2.calculation_5480628224156393 as DATE) <= date '2015-03-31') AND (CAST(realestate1_2.date_of_transfer as DATE) >= date '1995-01-01') AND (CAST(realestate1_2.date_of_transfer as DATE) <= date '2015-03-31') AND (CAST(EXTRACT(YEAR FROM realestate1_2.date_of_transfer) AS LONG) IN (2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014))) GROUP BY county;
