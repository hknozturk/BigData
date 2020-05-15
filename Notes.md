__DATA COLUMNS__

* marketplace       - 2 letter country code of the marketplace where the review was written.
* customer_id       - Random identifier that can be used to aggregate reviews written by a single author.

* review_id         - The unique ID of the review.
* product_id        - The unique Product ID the review pertains to. In the multilingual dataset the reviews
                    for the same product in different countries can be grouped by the same product_id.
* product_parent    - Random identifier that can be used to aggregate reviews for the same product.
* product_title     - Title of the product.
* product_category  - Broad product category that can be used to group reviews
                    (also used to group the dataset into coherent parts).
* star_rating       - The 1-5 star rating of the review.
* helpful_votes     - Number of helpful votes.
* total_votes       - Number of total votes the review received.
* vine              - Review was written as part of the Vine program. - https://www.amazon.com/gp/vine/help
* verified_purchase - The review is on a verified purchase.
* review_headline   - The title of the review.
* review_body       - The review text.
* review_date       - The date the review was written.

---

__DATA FORMAT__

Tab '\t' separated text file, without quote or escape characters.
First line in each file is header; 1 line corresponds to 1 record.

---

__NOTES__

Including product_category might be useful. This can help us to get more specific reviews (category specific reviews).

I am not sure start_rating is a good parameter to include. Good reviews can have low star rating (problem with product, shipment, ...).

** We can use helpful_votes / total_votes as a target variable.

---

__TO_DOs__

* implement quantile calculation for review body length categorization.
* I got null on cluster hdfs amazon dataset. Need to consider situations where wrong data type or missing data (null)
    * Problem is some reviews are no star rating.
* Remove star rating from features. (Reason: check second note)

---

__Appendix__

(__Table 1__): Execution time in s (converted from ns - cut) some cases calculating A is taking longer. For hold list check log.md

After changing A calculation from attributes.find(IG(data, target, _) == ig.max) to maxBy (A1)

| IG | A | A1 |
|----:|----:|----:|
|21.9|10.3|0.0001|
|13.5|9.5|8.688e-6|
|13.5|6.8|7e-9|
|5.1|4.8|7e-9|
|3.3|3.2|7e-9|
|5|2.5|6e-9|
|2.6|2.5|5e-9|
|1.9|1.9|5e-9|
|9.9|4.4|1e-8|
|5|5|5e-9|
|1.8|2.4|4e-9|
|2.5|2.5|7e-9|
---

__REFERENCES__

1. https://sparkbyexamples.com/
2. https://medium.com/@mrpowers/the-different-type-of-spark-functions-custom-transformations-column-functions-udfs-bf556c9d0ce7
3. https://stackoverflow.com/questions/45928007/use-withcolumn-with-external-function
4. https://stackoverflow.com/questions/37032025/how-to-sum-the-values-of-one-column-of-a-dataframe-in-spark-scala
5. https://blog.knoldus.com/machine-learning-with-decision-trees/
6. https://towardsdatascience.com/machine-learning-decision-tree-using-spark-for-layman-8eca054c8843
7. https://stackoverflow.com/questions/45755139/split-a-dataset-in-training-and-test-in-one-line-in-scala-spark
8. https://medium.com/greyatom/decision-trees-a-simple-way-to-visualize-a-decision-dc506a403aeb
9. https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
10. https://stackoverflow.com/questions/32199162/difference-between-spark-tolocaliterator-and-iterator-methods

__FURTHER READING__

* https://stackoverflow.com/questions/52521067/spark-dataframe-repartition-and-parquet-partition
* https://stackoverflow.com/questions/30995699/how-to-define-partitioning-of-dataframe