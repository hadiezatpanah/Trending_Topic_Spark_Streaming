GRANT ALL PRIVILEGES ON DATABASE brgroup to postgres;
\c brgroup;    
CREATE SCHEMA BRGROUP;
CREATE TABLE BRGROUP.TREND_TABLE (Trend_Rank int PRIMARY KEY, Topic_Name text, Trend_Score decimal, Load_Date timestamp);
