import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from utilities import SEED
# import any other dependencies you want, but make sure only to use the ones
# availiable on AWS EMR

# ---------------- choose input format, dataframe or rdd ----------------------
INPUT_FORMAT = 'dataframe'  # change to 'rdd' if you wish to use rdd inputs
# -----------------------------------------------------------------------------
if INPUT_FORMAT == 'dataframe':
    import pyspark.ml as M
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
if INPUT_FORMAT == 'koalas':
    import databricks.koalas as ks
elif INPUT_FORMAT == 'rdd':
    import pyspark.mllib as M
    from pyspark.mllib.feature import Word2Vec
    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.linalg.distributed import RowMatrix
    from pyspark.mllib.tree import DecisionTree
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.linalg import DenseVector
    from pyspark.mllib.evaluation import RegressionMetrics


# ---------- Begin definition of helper functions, if you need any ------------

# def task_1_helper():
#   pass

# -----------------------------------------------------------------------------


def task_1(data_io, review_data, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    overall_column = 'overall'
    # Outputs:
    mean_rating_column = 'meanRating'
    count_rating_column = 'countRating'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    
    temp_df1a = review_data\
                    .select(review_data[asin_column].alias('blah_asin'),
                            overall_column
                           )\
                    .groupby('blah_asin')\
                    .agg(F.avg(overall_column).alias(mean_rating_column),
                         F.count(overall_column).alias(count_rating_column)
                        )
    temp_df = product_data\
                    .join(temp_df1a, 
                          product_data[asin_column] == temp_df1a['blah_asin'],
                          how = 'left'
                         )\
                    .drop('blah_asin')
        
    mR1 = temp_df\
            .agg(F.count(asin_column).alias('count_total'),
                 F.avg(mean_rating_column).alias('mean_meanRating'),
                 F.variance(mean_rating_column).alias('variance_meanRating'),
                 F.avg(count_rating_column).alias('mean_countRating'),
                 F.variance(count_rating_column).alias('variance_countRating'),
                 F.count(F.when(F.col(mean_rating_column).isNull(),1)).alias('numNulls_meanRating'),
                 F.count(F.when(F.col(count_rating_column).isNull(),1)).alias('numNulls_countRating')
                ).cache()


    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    # Calculate the values programmaticly. Do not change the keys and do not
    # hard-code values in the dict. Your submission will be evaluated with
    # different inputs.
    # Modify the values of the following dictionary accordingly.
    res = {
        'count_total': None,
        'mean_meanRating': None,
        'variance_meanRating': None,
        'numNulls_meanRating': None,
        'mean_countRating': None,
        'variance_countRating': None,
        'numNulls_countRating': None
    }
    # Modify res:

    res['count_total'] = mR1.select('count_total').head()[0]
    res['mean_meanRating'] = mR1.select('mean_meanRating').head()[0]
    res['variance_meanRating'] = mR1.select('variance_meanRating').head()[0]
    res['numNulls_meanRating'] = mR1.select('numNulls_meanRating').head()[0]
    res['mean_countRating'] = mR1.select('mean_countRating').head()[0]
    res['variance_countRating'] = mR1.select('variance_countRating').head()[0]
    res['numNulls_countRating'] = mR1.select('numNulls_countRating').head()[0]
    

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_1')
    return res
    # -------------------------------------------------------------------------


def task_2(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    salesRank_column = 'salesRank'
    categories_column = 'categories'
    asin_column = 'asin'
    # Outputs:
    category_column = 'category'
    bestSalesCategory_column = 'bestSalesCategory'
    bestSalesRank_column = 'bestSalesRank'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    temp_df2 = product_data.withColumns({category_column:\
                                             F.when(F.col(categories_column)[0][0] == '', None)\
                                             .otherwise(F.col(categories_column)[0][0]),
                                         bestSalesCategory_column:F.map_keys(F.col(salesRank_column)).getItem(0),
                                         bestSalesRank_column:F.map_values(F.col(salesRank_column)).getItem(0)
                                        }
                                       )
    mR2 = temp_df2\
            .agg(F.count(asin_column).alias('count_total'),
                 F.avg(bestSalesRank_column).alias('mean_bestSalesRank'),
                 F.variance(bestSalesRank_column).alias('variance_bestSalesRank'),
                 F.count(F.when(F.col(category_column).isNull(),1)).alias('numNulls_category'),
                 F.count(F.when(F.col(bestSalesCategory_column).isNull(),1)).alias('numNulls_bestSalesCategory'),
                 F.count_distinct(F.when(F.col(category_column).isNotNull()
                                         & (F.col(category_column) != ''),
                                         F.col(category_column)
                                        )).alias('countDistinct_category'),
                 F.count_distinct(F.when(F.col(bestSalesCategory_column).isNotNull()
                                         & (F.col(bestSalesCategory_column) != ''),
                                         F.col(bestSalesCategory_column)
                                        )).alias('countDistinct_bestSalesCategory'),             
                ).cache()

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_bestSalesRank': None,
        'variance_bestSalesRank': None,
        'numNulls_category': None,
        'countDistinct_category': None,
        'numNulls_bestSalesCategory': None,
        'countDistinct_bestSalesCategory': None
    }
    # Modify res:

    res['count_total'] = mR2.select('count_total').head()[0]
    res['mean_bestSalesRank'] = mR2.select('mean_bestSalesRank').head()[0]
    res['variance_bestSalesRank'] = mR2.select('variance_bestSalesRank').head()[0]
    res['numNulls_category'] = mR2.select('numNulls_category').head()[0]
    res['countDistinct_category'] = mR2.select('countDistinct_category').head()[0]
    res['numNulls_bestSalesCategory'] = mR2.select('numNulls_bestSalesCategory').head()[0]
    res['countDistinct_bestSalesCategory'] = mR2.select('countDistinct_bestSalesCategory').head()[0]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_2')
    return res
    # -------------------------------------------------------------------------


def task_3(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    price_column = 'price'
    attribute = 'also_viewed'
    related_column = 'related'
    # Outputs:
    meanPriceAlsoViewed_column = 'meanPriceAlsoViewed'
    countAlsoViewed_column = 'countAlsoViewed'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    temp_df3 = product_data\
                    .select(asin_column,
                            F.explode(
                            F.map_values(
                                F.map_filter(related_column, lambda k,v: k==attribute)))\
                            .alias('temp'))\
                    .withColumn(attribute, 
                                F.explode(F.col('temp')))\
                    .select(asin_column,attribute)

    temp_df3a = product_data.select(F.col(asin_column).alias('also_asin'),price_column)
    
    temp_df3 = temp_df3\
                    .join(temp_df3a, 
                          temp_df3[attribute] == temp_df3a['also_asin'], 
                          how = 'left')\
                    .select(temp_df3[asin_column].alias('blah_asin'),
                            attribute,
                            price_column
                           )\
                    .groupby('blah_asin')\
                    .agg(F.avg(price_column).alias(meanPriceAlsoViewed_column),
                         F.count('blah_asin').alias(countAlsoViewed_column)
                        )
    
    temp_df3 = product_data\
                .join(temp_df3,
                      product_data[asin_column] == temp_df3['blah_asin'],
                      how = 'left')\
                .drop('blah_asin')

    mR3 = temp_df3\
            .agg(F.count(asin_column).alias('count_total'),
                 F.avg(meanPriceAlsoViewed_column).alias('mean_meanPriceAlsoViewed'),
                 F.variance(meanPriceAlsoViewed_column).alias('variance_meanPriceAlsoViewed'),
                 F.avg(countAlsoViewed_column).alias('mean_countAlsoViewed'),
                 F.variance(countAlsoViewed_column).alias('variance_countAlsoViewed'),
                 F.count(F.when(F.col(meanPriceAlsoViewed_column).isNull(),1)).alias('numNulls_meanPriceAlsoViewed'),
                 F.count(F.when(F.col(countAlsoViewed_column).isNull(),1)).alias('numNulls_countAlsoViewed')
                ).cache()

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanPriceAlsoViewed': None,
        'variance_meanPriceAlsoViewed': None,
        'numNulls_meanPriceAlsoViewed': None,
        'mean_countAlsoViewed': None,
        'variance_countAlsoViewed': None,
        'numNulls_countAlsoViewed': None
    }
    # Modify res:

    res['count_total'] = mR3.select('count_total').head()[0]
    res['mean_meanPriceAlsoViewed'] = mR3.select('mean_meanPriceAlsoViewed').head()[0]
    res['variance_meanPriceAlsoViewed'] = mR3.select('variance_meanPriceAlsoViewed').head()[0]
    res['numNulls_meanPriceAlsoViewed'] = mR3.select('numNulls_meanPriceAlsoViewed').head()[0]
    res['mean_countAlsoViewed'] = mR3.select('mean_countAlsoViewed').head()[0]
    res['variance_countAlsoViewed'] = mR3.select('variance_countAlsoViewed').head()[0]
    res['numNulls_countAlsoViewed'] = mR3.select('numNulls_countAlsoViewed').head()[0]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_3')
    return res
    # -------------------------------------------------------------------------


def task_4(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    price_column = 'price'
    title_column = 'title'
    # Outputs:
    meanImputedPrice_column = 'meanImputedPrice'
    medianImputedPrice_column = 'medianImputedPrice'
    unknownImputedTitle_column = 'unknownImputedTitle'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    temp_df4 = product_data.withColumn(price_column, F.col(price_column).cast('float'))
    
    im1 = M.feature.Imputer(inputCol = price_column, 
                            outputCol = meanImputedPrice_column, 
                            strategy = 'mean'
                           )
    im2 = M.feature.Imputer(inputCol = price_column, 
                            outputCol = medianImputedPrice_column, 
                            strategy = 'median'
                           )

    temp_df4 = im1.fit(temp_df4).transform(temp_df4)
    temp_df4 = im2.fit(temp_df4).transform(temp_df4)

    temp_df4 = temp_df4.withColumn(unknownImputedTitle_column, 
                                   F.when(F.col(title_column) == '',None)\
                                       .otherwise(F.col(title_column))
                                  )\
                    .fillna('unknown',unknownImputedTitle_column)
    
    mR4 = temp_df4\
            .agg(F.avg(meanImputedPrice_column).alias('mean_meanImputedPrice'),
                 F.variance(meanImputedPrice_column).alias('variance_meanImputedPrice'),
                 F.avg(medianImputedPrice_column).alias('mean_medianImputedPrice'),
                 F.variance(medianImputedPrice_column).alias('variance_medianImputedPrice'),
                 F.count(F.when(F.col(meanImputedPrice_column).isNull(),1)).alias('numNulls_meanImputedPrice'),
                 F.count(F.when(F.col(medianImputedPrice_column).isNull(),1)).alias('numNulls_medianImputedPrice'),
                 F.count(F.when(F.col(unknownImputedTitle_column)=='unknown',1)).alias('numUnknowns_unknownImputedTitle')
                ).cache()

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanImputedPrice': None,
        'variance_meanImputedPrice': None,
        'numNulls_meanImputedPrice': None,
        'mean_medianImputedPrice': None,
        'variance_medianImputedPrice': None,
        'numNulls_medianImputedPrice': None,
        'numUnknowns_unknownImputedTitle': None
    }
    # Modify res:

    res['count_total'] = temp_df4.count()
    res['mean_meanImputedPrice'] = mR4.select('mean_meanImputedPrice').head()[0]
    res['variance_meanImputedPrice'] = mR4.select('variance_meanImputedPrice').head()[0]
    res['numNulls_meanImputedPrice'] = mR4.select('numNulls_meanImputedPrice').head()[0]
    res['mean_medianImputedPrice'] = mR4.select('mean_medianImputedPrice').head()[0]
    res['variance_medianImputedPrice'] = mR4.select('variance_medianImputedPrice').head()[0]
    res['numNulls_medianImputedPrice'] = mR4.select('numNulls_medianImputedPrice').head()[0]
    res['numUnknowns_unknownImputedTitle'] = mR4.select('numUnknowns_unknownImputedTitle').head()[0]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_4')
    return res
    # -------------------------------------------------------------------------


def task_5(data_io, product_processed_data, word_0, word_1, word_2):
    # -----------------------------Column names--------------------------------
    # Inputs:
    title_column = 'title'
    # Outputs:
    titleArray_column = 'titleArray'
    titleVector_column = 'titleVector'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    temp_df5 = product_processed_data.withColumn(titleArray_column, 
                                                 F.split(F.lower(F.col(title_column)), ' ')
                                                )
    
    w2V = M.feature.Word2Vec(vectorSize = 16, 
                             minCount = 100,
                             seed = SEED,
                             numPartitions = 4,
                             inputCol = titleArray_column
                            )
    model = w2V.fit(temp_df5)

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'size_vocabulary': None,
        'word_0_synonyms': [(None, None), ],
        'word_1_synonyms': [(None, None), ],
        'word_2_synonyms': [(None, None), ]
    }
    # Modify res:
    
    res['count_total'] = temp_df5.count()
    res['size_vocabulary'] = model.getVectors().count()
    for name, word in zip(
        ['word_0_synonyms', 'word_1_synonyms', 'word_2_synonyms'],
        [word_0, word_1, word_2]
    ):
        res[name] = model.findSynonymsArray(word, 10)

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_5')
    return res
    # -------------------------------------------------------------------------


def task_6(data_io, product_processed_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    category_column = 'category'
    # Outputs:
    categoryIndex_column = 'categoryIndex'
    categoryOneHot_column = 'categoryOneHot'
    categoryPCA_column = 'categoryPCA'
    # -------------------------------------------------------------------------    

    # ---------------------- Your implementation begins------------------------

    temp_df6 = product_processed_data

    temp_df6 = M.feature.StringIndexer(inputCol = category_column,
                                       outputCol = categoryIndex_column
                                      )\
                                    .fit(temp_df6)\
                                    .transform(temp_df6)

    temp_df6 = M.feature.OneHotEncoder(inputCol = categoryIndex_column,
                                       outputCol = categoryOneHot_column,
                                       dropLast = False
                                      )\
                                    .fit(temp_df6)\
                                    .transform(temp_df6)

    temp_df6 = M.feature.PCA(inputCol = categoryOneHot_column,
                             outputCol = categoryPCA_column,
                             k = 15
                            )\
                            .fit(temp_df6)\
                            .transform(temp_df6)

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'meanVector_categoryOneHot': [None, ],
        'meanVector_categoryPCA': [None, ]
    }
    # Modify res:

    res['count_total'] = temp_df6.count()
    res['meanVector_categoryOneHot'] = temp_df6.select(M.stat.Summarizer.mean(F.col(categoryOneHot_column))).head()[0]
    res['meanVector_categoryPCA'] = temp_df6.select(M.stat.Summarizer.mean(F.col(categoryPCA_column))).head()[0]

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_6')
    return res
    # -------------------------------------------------------------------------
    
    
def task_7(data_io, train_data, test_data):
    
    # ---------------------- Your implementation begins------------------------
    
    reg = M.regression.DecisionTreeRegressor(featuresCol = 'features',
                                             labelCol = 'overall',
                                             predictionCol = 'prediction',
                                             maxDepth = 5,
                                            )\
                                            .fit(train_data)    
    pred = reg.transform(test_data)
    rmse = M.evaluation.RegressionEvaluator(predictionCol = 'prediction',
                                            labelCol = 'overall',
                                            metricName = 'rmse'
                                           )\
                                            .evaluate(pred)      
    
    # -------------------------------------------------------------------------
    
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None
    }
    # Modify res:

    res['test_rmse'] = rmse

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_7')
    return res
    # -------------------------------------------------------------------------
    
    
def task_8(data_io, train_data, test_data):
    
    # ---------------------- Your implementation begins------------------------
    
    train_data, valid_data = train_data.randomSplit([0.75,0.25])
    
    best_rmse = 1e6
    evaluator = M.evaluation.RegressionEvaluator(predictionCol = 'prediction',
                                                 labelCol = 'overall',
                                                 metricName = 'rmse'
                                                )    
    
    # -------------------------------------------------------------------------
    
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None,
        'valid_rmse_depth_5': None,
        'valid_rmse_depth_7': None,
        'valid_rmse_depth_9': None,
        'valid_rmse_depth_12': None,
    }
    # Modify res:

    for k in [5,7,9,12]:
        reg = M.regression.DecisionTreeRegressor(featuresCol = 'features',
                                             labelCol = 'overall',
                                             predictionCol = 'prediction',
                                             maxDepth = k,
                                            )\
                                            .fit(train_data)
        pred = reg.transform(valid_data)
        rmse = evaluator.evaluate(pred)
        
        res['valid_rmse_depth_{}'.format(k)] = rmse

        if rmse < best_rmse:
            best_depth = k
            best_reg = reg

    test_pred = best_reg.transform(test_data)
    res['test_rmse'] = evaluator.evaluate(test_pred)

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_8')
    return res
    # -------------------------------------------------------------------------

