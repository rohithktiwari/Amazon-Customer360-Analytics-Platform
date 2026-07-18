-- Total Customers

SELECT COUNT(*)
FROM amazon_customer360.gold.customer360;

-- Null Customer IDs

SELECT COUNT(*)
FROM amazon_customer360.gold.customer360
WHERE Customer_ID IS NULL;

-- Total Transaction Amount

SELECT SUM(Total_Amount)
FROM amazon_customer360.gold.customer360;

-- Customer Segments

SELECT Segment, COUNT(*)
FROM amazon_customer360.gold.customer360
GROUP BY Segment;

-- Membership Distribution

SELECT Membership, COUNT(*)
FROM amazon_customer360.gold.customer360
GROUP BY Membership;
