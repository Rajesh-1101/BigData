/*
Problem: Find stores that were opened in the second half of 2021 with more than 20% of their reviews being negative. 
A review is considered negative when the score given by a customer is below 5. Output the names of the stores together
with the ratio of negative reviews to positive ones.

Schema :
Table 1: instacart_reviews

id: int
customer_id : int
store_id : int
score: int

Table 2 : instacart_stores

id: int
name: varchar
zipcode: int
opening_date : DateTime
*/

-- Step 1: Filter stores opened in the second half of 2021
WITH filter_store AS (
    SELECT id, name
    FROM instacart_stores 
    WHERE opening_date BETWEEN '2021-07-01' AND '2021-12-31'
),

-- Step 2: Calculate the number of negative and positive reviews for each store
store_review AS (
    SELECT 
        r.store_id,
        SUM(CASE WHEN r.score < 5 THEN 1 ELSE 0 END) AS negative_reviews,
        SUM(CASE WHEN r.score >= 5 THEN 1 ELSE 0 END) AS positive_reviews
    FROM instacart_reviews r
    JOIN filter_store s ON s.id = r.store_id
    GROUP BY r.store_id
),

-- Step 3: Calculate the ratio of negative to positive reviews for each store
store_ratios AS (
    SELECT
        s.name,
        sr.negative_reviews,
        sr.positive_reviews,
        (sr.negative_reviews * 1.0 / sr.positive_reviews) AS negative_to_positive_ratio
    FROM store_review sr
    JOIN filter_store s ON sr.store_id = s.id
    WHERE sr.negative_reviews > 0 -- Ensure we are not dividing by zero
)

-- Step 4: Filter stores where more than 20% of reviews are negative and output the results
SELECT name, negative_to_positive_ratio
FROM store_ratios
WHERE (negative_reviews * 1.0 / (negative_reviews + positive_reviews)) > 0.20;

