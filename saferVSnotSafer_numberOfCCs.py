# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 15:42:25 2026

@author: BChung

This script is an analysis of the BCPP dataset. In particular, I'm trying to
see whether there is a difference in the number of CCs between products that
are deemed as safer and products that are deemed as not safer. I'm also trying
to see whether there is a difference in hazard traits between products that are
safer and products that are not.

To do this analysis, I'll only be looking at the 2022 data.

"""
import os
from pathlib import Path
import pandas as pd
from scipy import stats
import statsmodels.api as sm
import math
import itertools
from matplotlib import pyplot as plt
import matplotlib as mpl

repository = Path(os.getcwd())
repositoryFolder = Path(os.path.dirname(repository))
dataFolder = repositoryFolder/"Data"
inputFolder = dataFolder/"Input"
outputFolder = dataFolder/"Output"

"""Reading in the 2022 data and the CC list."""
dataPath = outputFolder/"BCPP, cleaned and identified ingredients & product data.xlsx"
data2022string = ["productID", "Brand", "Product name", "Breadcrumbs",
                  "Product Category", "Product description",
                  "SAFER BLACK BEAUTY BRAND PRODUCT?", "ingredientList",
                  "ingredient2", "identified", "DTXSID", "PREFERRED_NAME",
                  "CompToxCASRN", "SMILES", "identificationMethod", "CosIngCASRN"]
data2022dtypes = {field: "string" for field in data2022string}
data2022dtypes.update({"ingredientOrder": "int"})
data2022 = (pd.read_excel(dataPath, "2022 dataset", dtype=data2022dtypes)
            .rename(columns={"SAFER BLACK BEAUTY BRAND PRODUCT?": "safer"})
            )
CClistPath = inputFolder/"CalSAFER_CandidateChemicals_2026-06-25.xlsx"
CClist = (pd.read_excel(CClistPath, header=7, usecols=[0, 1, 3, 5, 6],
                        dtype="string")
          .rename(columns={"Chemical Name": "CC", "CAS RN": "CASRN", "Authoritative List": "AuthoritativeList", "Hazard Traits": "HazardTrait"})
          )
CClist.loc[CClist.CASRN == "No CAS RN", "CASRN"] = pd.NA
CClist.loc[CClist.CASRN.notna(), "CASRN"] = CClist.CASRN.str.strip()
# %%
data2022identified = data2022.query("DTXSID.notna()")

"""I will prepare to merge the CC list with the 2022 data. To do so, I need to
find DTXSIDs for the CCs. It also might be helpful to look through the synonyms
for CAS RNs and search those as well."""
CCs = (CClist.drop(columns=["AuthoritativeList", "HazardTrait"])
       .drop_duplicates()
       .reset_index(drop=True)
       )
CCs.loc[CCs.CASRN.isna(), "CASRN"] = ""

CCsynonymsSplit = CCs.Synonyms.str.split("; ", expand=True, regex=False)
CCwithSynonyms = (CCs.join(CCsynonymsSplit)
                  .query("Synonyms.notna()")
                  .melt(["CC", "CASRN", "Synonyms"], var_name="synonymOrder", value_name="synonym")
                  .drop(columns=["synonymOrder"])
                  .query("synonym.notna()")
                  .drop_duplicates()
                  )
CASRNregex = r"[1-9][0-9]{1,6}-[0-9]{2}-[0-9](?!\d)"
CASRNasSynonym = CCwithSynonyms.loc[CCwithSynonyms.synonym.str.contains(CASRNregex)]
CASRNasSynonym2 = (CASRNasSynonym.synonym.str.extractall(r"([1-9][0-9]{1,6}-[0-9]{2}-[0-9](?!\d))")
                   .reset_index("match", drop=True)
                   .rename(columns={0: "otherCASRN"})
                   )
CASRNasSynonym = (CASRNasSynonym.join(CASRNasSynonym2)
                  .reset_index(drop=True)
                  )
CCwithSynonyms = CCwithSynonyms.merge(CASRNasSynonym, "left", ["CC", "CASRN", "Synonyms", "synonym"])
CCwithoutSynonyms = CCs.query("Synonyms.isna()")
CCs = (pd.concat([CCwithSynonyms, CCwithoutSynonyms], ignore_index=False)
       .drop_duplicates()
       )

CCs["combinedCASRN"] = CCs.CASRN + "|" + CCs.otherCASRN
CASRNsplit = (CCs.combinedCASRN.str.split("|", regex=False, expand=True)
              .dropna(how="all")
              )

CCs = (CCs.join(CASRNsplit)
       .melt(["CC", "CASRN", "Synonyms", "synonym", "otherCASRN", "combinedCASRN"], var_name="CASRNorder", value_name="allCASRN")
       .drop(columns=["combinedCASRN", "CASRNorder"])
       .drop_duplicates()
       )

CCs.loc[CCs.allCASRN.isna(), "allCASRN"] = CCs.CASRN
CCs.loc[CCs.allCASRN == "", "allCASRN"] = pd.NA
CCs.loc[CCs.CASRN == "", "CASRN"] = pd.NA
CCs = CCs.loc[~(CCs.otherCASRN.notna() & CCs.allCASRN.isna())]
CCs = CCs.drop_duplicates()
"""Gonna export a csv of these CAS RNs so I can copy and do a batch search of
them."""
CC_CASRNdf = (CCs.filter(["allCASRN"])
              .drop_duplicates()
              .query("allCASRN.notna()")
              )
CC_CASRNpath = outputFolder/"CAS RNs on the CC list - downloaded 6-25-2026.csv"
if os.path.exists(CC_CASRNpath) is False:
    CC_CASRNdf.to_csv(CC_CASRNpath, index=False)

"""Now that I exported all the CAS RNs on the CC list and performed a batch
search of them, let's pull import the batch search results."""
batchSearchPath = inputFolder/"CC list CAS RN batch search.xlsx"
batchSearch = (pd.read_excel(batchSearchPath, "Main Data",usecols=[0, 2, 3, 4],
                             dtype="string")
               .rename(columns={"INPUT": "allCASRN", "CASRN": "CompToxCASRN"})
               )
CCs = CCs.merge(batchSearch, "left", "allCASRN")
CClist2 = CClist.merge(CCs, "outer", ["CC", "CASRN", "Synonyms"])
# %%
"""Now that I've finally added DTXSIDs to CCs, let's combine the CC data with
the 2022 product - ingredient dataset and start analyzing.
"""
dataCCcombined = data2022identified.merge(CClist2, "left", ["DTXSID", "PREFERRED_NAME", "CompToxCASRN"], indicator=True)
dataCCcombined.loc[dataCCcombined._merge == "left_only", "CCstatus"] = "Not CC"
dataCCcombined.loc[dataCCcombined._merge == "both", "CCstatus"] = "CC"
dataCCcombined = (dataCCcombined.drop(columns=["_merge"])
                  .drop_duplicates()
                  )
dataCCcombined.loc[dataCCcombined["Product Category"].isna(), "Product Category"] = "Unassigned"

"""Let's calculate the number of CCs per product. I will analyze this value
later."""
dataWithCCs = dataCCcombined.query("CCstatus == 'CC'")
dataWithoutCCs1 = dataCCcombined.query("CCstatus == 'Not CC'")

CCsPerProduct = (dataCCcombined.groupby(["productID", "CCstatus"])["DTXSID"].nunique()
                 .reset_index()
                 .rename(columns={"DTXSID": "numberOfCCs"})
                 .query("CCstatus == 'CC'")
                 .drop(columns=["CCstatus"])
                 )
"""This count doesn't include any 0s whatsoever, so if a product only contains
non-CCs, then this product isn't listed in CCsPerProduct. Because it omits
products like these, I will need to manually set their CC count to 0."""
CCsPerProduct_Data = (CCsPerProduct.merge(dataCCcombined, "right", "productID")
                      .filter(["productID", "numberOfCCs", "Product Category", "safer"])
                      .drop_duplicates()
                      )
CCsPerProduct_Data.loc[CCsPerProduct_Data.numberOfCCs.isna(), "numberOfCCs"] = 0
"""Sanity check to see if the products that I assigned as 0 CCs to, actually do
not have any CCs"""
notActuallyZeroCCs = []
assignedZeroCCs = CCsPerProduct_Data.loc[CCsPerProduct_Data.numberOfCCs == 0, "productID"].drop_duplicates().tolist()
for product in assignedZeroCCs:
    productDF = dataWithCCs.loc[dataWithCCs.productID == product]
    if productDF.empty is False:
        notActuallyZeroCCs.append(product)
"""notActuallyZeroCCs is an empty list, so the products that I assigned as 0
CCs actually do not have any CCs."""

"""I want to conduct a simple 2-sample t-test between safer products and
products that are not safer with the variable I'm testing being the number of
CCs.

Assumptions of a 2-sample t-test
- Data satisfies normality, meaning that data is likely drawn from a normally
distributed population
https://doi.org/10.4097/kja.d.18.00292
- homogeneity of variance, the same variance between both groups
    - t-test is robust to unequal variances if the 2 samples have the same
    sizes but not robust if the 2 samples have totally different sizes
    https://pages.stat.wisc.edu/~st571-1/Fall2005/lec18-21.1.pdf
    
- independence

You can use a Welch's t-test when the samples both satisfy normality but have
different variances
https://en.wikipedia.org/wiki/Welch%27s_t-test

Use nonparametric methods if the above assumptions aren't meant. Nonparametric
methods make very few assumptions about the underlying distributions of the
data
https://en.wikipedia.org/wiki/Nonparametric_statistics

I can use the Mann-Whitney U test and the Kruskal-Wallis test if the
assumptions above fail. However, the Mann-Whitney U test might not be too
robust against skewed or zero-inflated data
https://journals.physiology.org/doi/full/10.1152/advan.00017.2010
"""
saferOrNotTotals = (data2022identified.groupby("safer")["productID"].nunique()
                    .reset_index()
                    .rename(columns={"productID": "totalProductsPerSaferNotSafer"})
                    )
distributionCCsPerProduct = (CCsPerProduct_Data.groupby(["safer", "numberOfCCs"])["productID"].nunique()
                             .reset_index()
                             .rename(columns={"productID": "numberOfProducts"})
                             .merge(saferOrNotTotals, "inner", "safer")
                             )
distributionCCsPerProduct["fraction"] = distributionCCsPerProduct.numberOfProducts/distributionCCsPerProduct.totalProductsPerSaferNotSafer
"""Ok, the distribution for each of the 2 groups (safer products vs not safer)
are extremely right-skewed, with the plurality being products not having any
CCs. So, the assumption of normality is violated. I'll just proceed on with the
Mann-Whitney U test.
"""

mannWhitney = stats.mannwhitneyu(CCsPerProduct_Data.loc[CCsPerProduct_Data.safer == "Yes", "numberOfCCs"], CCsPerProduct_Data.loc[CCsPerProduct_Data.safer == "No", "numberOfCCs"])
"""Mann-Whitney U p-value of 2.98087 * 10^-72, extremely tiny p-value,
indicating that the 2 groups are statistically significantly different."""

"""Let's use a couple of different discrete regression models from statsmodels
here.

- Poisson
- Negative Binomial (NB)
- Generalized Negative Binomial (NBP)
- zero-inflated Poisson (ZIP)
- zero-inflated Generalized Negative Binomial (ZNBP)

AIC and BIC can be calculated as the following
https://pmc.ncbi.nlm.nih.gov/articles/PMC11056604/
"""
CCsPerProduct_Data_regress = (CCsPerProduct_Data.copy()
                              .astype({"safer": "category", "Product Category": "category"})
                              .rename(columns={"Product Category": "productCategory"})
                              )

modelString = "numberOfCCs ~ C(safer)"

CCsPerProduct_Poisson = sm.Poisson.from_formula(modelString, CCsPerProduct_Data_regress)
CCsPerProduct_Poisson_AIC = (2*2) - (2* (-9680.6))
CCsPerProduct_Poisson_BIC = (2*math.log(7152)) - (2*(-9680.6))

CCsPerProduct_NB = sm.NegativeBinomial.from_formula(modelString, CCsPerProduct_Data_regress)
CCsPerProduct_NB_AIC = (2*3) - (2* (-9250.3))
CCsPerProduct_NB_BIC = (3*math.log(7152)) - (2*(-9250.3))

CCsPerProduct_NBP = sm.NegativeBinomialP.from_formula(modelString, CCsPerProduct_Data_regress)
CCsPerProduct_NBP_AIC = (2*3) - (2* (-9250.3))
CCsPerProduct_NBP_BIC = (3*math.log(7152)) - (2*(-9250.3))

CCsPerProduct_ZIP = sm.ZeroInflatedPoisson.from_formula(modelString, CCsPerProduct_Data_regress)
CCsPerProduct_ZIP_AIC = (2*3) - (2* (-9313.5))
CCsPerProduct_ZIP_BIC = (3*math.log(7152)) - (2*(-9313.5))

CCsPerProduct_ZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString, CCsPerProduct_Data_regress)
CCsPerProduct_ZNBP_AIC = (2*4) - (2* (-9245.9))
CCsPerProduct_ZNBP_BIC = (4*math.log(7152)) - (2*(-9245.9))
CCsPerProduct_models = ["Poisson", "Negative Binomial", "Generalized Negative Binomial",
                        "zero-inflated Poisson", "zero-inflated Generalized Negative Binomial"]
CCsPerProduct_AIC = [CCsPerProduct_Poisson_AIC, CCsPerProduct_NB_AIC, CCsPerProduct_NBP_AIC,
                     CCsPerProduct_ZIP_AIC, CCsPerProduct_ZNBP_AIC]

CCsPerProduct_BIC = [CCsPerProduct_Poisson_BIC, CCsPerProduct_NB_BIC, CCsPerProduct_NBP_BIC,
                     CCsPerProduct_ZIP_BIC, CCsPerProduct_ZNBP_BIC]
CCsPerProduct_modelsDF = (pd.DataFrame({"models": CCsPerProduct_models,
                                        "AIC": CCsPerProduct_AIC,
                                        "BIC": CCsPerProduct_BIC})
                          .sort_values(["AIC", "BIC", "models"], ignore_index=True)
                          )

"""A regular Poisson model has the highest AIC and BIC, so this model should be
dropped. Zero-inflated Poisson has lower AIC & BIC values, but both values for
this model are still higher than the remaining 3's, so probably should be
dropped. The last 3 are variations of the negative binomial model. NBP & NB
both have identical AIC & BIC. I'll use the zero-inflated Generalized Negative
Binomial model due to an AIC that's 6.8 points lower than NB & NBP while having
a BIC that's just 0.1 higher than NB & NBP"""
CCsPerProduct_ZNBPfit = CCsPerProduct_ZNBP.fit()
CCsPerProduct_ZNBPsummary = CCsPerProduct_ZNBPfit.summary()
"""Tiny ass p-value, less than 0.001. Indicates that there is a significant
difference in the number of CCs per product between 'safer' and 'not safer'
products."""


CCsPerProduct_mean = CCsPerProduct_Data.groupby("safer")["numberOfCCs"].mean()
"""Products that are not safer, on average, contains 0.998 CCs per product
while products that are safer contains 0.129 CCs per product. So products that
are not safer contains more CCs. This difference seems statistically
significant according to the Mann-Whitney U test. Not altogether surprising."""
# %%
"""Let's look at this by hazard traits now. Also need to look at product
categories eventually."""

"""Let's see what kind of hazard traits there are"""
hazardTraitCounts = (dataWithCCs.groupby("HazardTrait")["productID"].nunique()
                     .sort_values(ascending=False)
                     .reset_index()
                     .rename(columns={"productID": "numberOfProducts"})
                     )

hazardTraits = hazardTraitCounts.HazardTrait.drop_duplicates().tolist()

hazardTraitCounts_Data = (dataWithCCs.groupby(["productID", "HazardTrait"])["DTXSID"].nunique()
                          .reset_index()
                          .rename(columns={"DTXSID": "numberOfChemicals"})
                          .merge(dataCCcombined, "right", ["productID", "HazardTrait"])
                          .filter(["productID", "HazardTrait", "numberOfChemicals", "Product Category", "safer"])
                          .query("HazardTrait.notna()")
                          .drop_duplicates()
                          )
hazardTraitCounts_Distribution = (hazardTraitCounts_Data.groupby(["safer", "HazardTrait", "numberOfChemicals"])["productID"].nunique()
                                  .reset_index()
                                  .rename(columns={"productID": "numberOfProducts"})
                                  .sort_values(["HazardTrait", "safer", "numberOfChemicals"], ascending=True)
                                  )
"""Quite interesting to see that there are certain hazard traits that are only
in products that are not safer."""
hazardTraitsInNotSafer = set(hazardTraitCounts_Distribution.loc[hazardTraitCounts_Distribution.safer == "No", "HazardTrait"].tolist())
hazardTraitsInSafer = set(hazardTraitCounts_Distribution.loc[hazardTraitCounts_Distribution.safer == "Yes", "HazardTrait"].tolist())
hazardTraitsOnlyInNotSafer = hazardTraitsInNotSafer - hazardTraitsInSafer
hazardTraitsOnlyInSafer = hazardTraitsInSafer - hazardTraitsInNotSafer
hazardTraitsBoth = hazardTraitsInNotSafer & hazardTraitsInSafer
hazardTraitsBothList = list(hazardTraitsInNotSafer & hazardTraitsInSafer)
"""Products deemed as safer don't contain hazard traits unique to themselves
while products that are not deemed as safer contain 8 hazard traits deemed as
unique to themselves. The 'not safer' products contain a total of 15 hazard
traits while the 'safer' products contain a total of 7 hazard traits, all of
which are in products deemed as 'not safer'

I'll do a Mann-Whitney U for each of the 7 hazard traits that are in common
between safer and 'not safer' products
"""
hazardTraitCounts_Data2 = hazardTraitCounts_Data.query("HazardTrait == @hazardTraitsBothList")
hazardTraitCounts_Distribution2 = hazardTraitCounts_Distribution.query("HazardTrait == @hazardTraitsBothList")

"""For products with any of the 7 hazard traits above, I will see if each
product is missing any of the 7 hazard traits, and for each missing hazard
trait, I will assign the number of chemicals with that hazard trait as 0."""
fillingMissingHazardTraits = []
productsWithCommonHazardTraits = hazardTraitCounts_Data2.productID.drop_duplicates().tolist()
for product in productsWithCommonHazardTraits:
    productDF = hazardTraitCounts_Data2.loc[hazardTraitCounts_Data2.productID == product]
    availableHazards = set(productDF.HazardTrait.tolist())
    missingHazards = hazardTraitsBoth - availableHazards
    if len(missingHazards) >= 1:
        for missingHazard in missingHazards:
            fillingMissingHazardTraits.append([product, missingHazard, 0])
fillingMissingHazardTraitsDF = pd.DataFrame(fillingMissingHazardTraits, columns=["productID", "HazardTrait", "numberOfChemicals"])
hazardTraitCounts_Data3 = (pd.concat([hazardTraitCounts_Data2, fillingMissingHazardTraitsDF],
                                     ignore_index=True)
                           .sort_values(["productID", "Product Category", "safer"], ignore_index=True)
                           .ffill()
                           )

"""So I identified 7,152 products. Of these, there are 3,455 products with at
least 1 of the 7 hazard traits that are present in both 'safer' products and
'not safer' products. That leaves 7,152 - 3,455 = 3,697 products without any
chemicals with these hazard traits. Going to manually assign them 0 CCs for
each of these 7 hazard traits."""
productsNoCommonHazardTraits = list(set(dataCCcombined.productID.drop_duplicates().tolist()) - set(productsWithCommonHazardTraits))
productsNoCommonHazardTraitsDF = (dataCCcombined.query("productID == @productsNoCommonHazardTraits")
                                  .filter(["productID", "Product Category", "safer"])
                                  .drop_duplicates()
                                  )
productsNoCommonHazardTraitsList = []
for hazardTrait in hazardTraitsBoth:
    df = productsNoCommonHazardTraitsDF.copy()
    df["HazardTrait"] = hazardTrait
    df["numberOfChemicals"] = 0
    productsNoCommonHazardTraitsList.append(df)
productsNoCommonHazardTraitsDF = pd.concat(productsNoCommonHazardTraitsList, ignore_index=True)

"""And I finished filling in 0s for products without any of the 7 hazard
traits. Now I finally have a dataset that's ready for analysis of hazard
traits."""
hazardTraitCounts_Data4 = (pd.concat([hazardTraitCounts_Data3, productsNoCommonHazardTraitsDF],
                                     ignore_index=True)
                           .sort_values(["productID", "HazardTrait"], ignore_index=True)
                           .astype({"safer": "category"})
                           )
# %%
"""And now, let's perform regression on each of the 7 hazard traits. 5 models
and 7 hazard traits means 35 models in total. Yay.

Respiratory toxicity in this chunk
"""
modelString_hazardTraits = "numberOfChemicals ~ C(safer)"


def AIC(parameters, logLikelihood):
    """
    Calculates the Akaike Information Criterion (AIC) for a specific model and
    its fit to a dataset.

    Parameters
    ----------
    parameters : integer
        Number of parameters in the model.
    logLikelihood : float
        Natural log of the likelihood of the model's fit to the data.

    Returns
    -------
    Float representing AIC value.

    """
    return (2 * parameters) - (2 * logLikelihood)


def BIC(parameters, logLikelihood, sampleSize):
    """
    Calculates the Bayesian Information Criterion (BIC) for a specific model
    and its fit to a datset.

    Parameters
    ----------
    parameters : integer
        Number of parameters in the model.
    logLikelihood : float
        Natural log of the likelihood of the model's fit to the data.
    sampleSize : integer
        The sample size of the data used to fit the model.

    Returns
    -------
    Float representing BIC value.

    """
    return (parameters * math.log(sampleSize)) - (2 * logLikelihood)

respiratoryData = hazardTraitCounts_Data4.loc[hazardTraitCounts_Data4.HazardTrait == "Respiratory Toxicity"]
respiratory_Poisson = sm.Poisson.from_formula(modelString_hazardTraits, respiratoryData)
respiratory_Poisson_AIC = AIC(2, -2324.1)
respiratory_Poisson_BIC = BIC(2, -2324.1, 7152)

respiratory_NB = sm.NegativeBinomial.from_formula(modelString_hazardTraits, respiratoryData)
respiratory_NB_AIC = AIC(3, -2315)
respiratory_NB_BIC = BIC(3, -2315, 7152)

respiratory_NBP = sm.NegativeBinomialP.from_formula(modelString_hazardTraits, respiratoryData)
respiratory_NBP_AIC = AIC(3, -2315)
respiratory_NBP_BIC = BIC(3, -2315, 7152)

respiratory_ZIP = sm.ZeroInflatedPoisson.from_formula(modelString_hazardTraits, respiratoryData)
respiratory_ZIP_AIC = AIC(3, -2313.2)
respiratory_ZIP_BIC = BIC(3, -2313.2, 7152)

respiratory_ZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString_hazardTraits, respiratoryData)

"""The zero-inflated Generalized Negative Binomial model failed to fit, so
disregard this model."""

respiratory_Models = ["Poisson", "Negative Binomial",
                      "Generalized Negative Binomial",
                      "zero-inflated Poisson"]
respiratory_AIC = [respiratory_Poisson_AIC, respiratory_NB_AIC,
                   respiratory_NBP_AIC, respiratory_ZIP_AIC]
respiratory_BIC = [respiratory_Poisson_BIC, respiratory_NB_BIC,
                   respiratory_NBP_BIC, respiratory_ZIP_BIC]
respiratory_ModelsDF = (pd.DataFrame({"model": respiratory_Models,
                                      "AIC": respiratory_AIC,
                                      "BIC": respiratory_BIC})
                        .sort_values(["AIC", "BIC", "model"],
                                     ignore_index=True)
                        )
"""Zero-inflated Poisson outperformed the other 3 models in both AIC & BIC, so
use results from zero-inflated Poisson"""
respiratory_ZIP_fit = respiratory_ZIP.fit()
respiratory_ZIP_summary = respiratory_ZIP_fit.summary()
respiratory_Mean = respiratoryData.groupby("safer")["numberOfChemicals"].mean()
"""p-value = 0.010, so there's a difference between safer & not safer products
for respiratory toxicity, with the 'not safer' products having a higher number
of CCs with this hazard trait (0.0985 CCs per product) than the 'safer'
products
"""

# %%
"""Ocular toxicity next."""
ocularData = hazardTraitCounts_Data4.query("HazardTrait == 'Ocular Toxicity'")

ocularPoisson = sm.Poisson.from_formula(modelString_hazardTraits, ocularData)
ocularPoisson_AIC = AIC(2, -1948.2)
ocularPoisson_BIC = BIC(2, -1948.2, 7152)

ocularNB = sm.NegativeBinomial.from_formula(modelString_hazardTraits, ocularData)
ocularNB_AIC = AIC(3, -1947.9)
ocularNB_BIC = BIC(3, -1947.9, 7152)

ocularNBP = sm.NegativeBinomialP.from_formula(modelString_hazardTraits, ocularData)
ocularNBP_AIC = AIC(3, -1947.9)
ocularNBP_BIC = BIC(3, -1947.9, 7152)

ocularZIP = sm.ZeroInflatedPoisson.from_formula(modelString_hazardTraits, ocularData)
ocularZIP_AIC = AIC(3, -1947.8)
ocularZIP_BIC = BIC(3, -1947.8, 7152)

respiratory_ZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString_hazardTraits, ocularData)
"""The zero-inflated Generalized Negative Binomial model, again, failed to fit,
so disregard this model."""
ocularModels = ["Poisson", "Negative Binomial",
                "Generalized Negative Binomial", "zero-inflated Poisson"]
ocularAIC = [ocularPoisson_AIC, ocularNB_AIC, ocularNBP_AIC, ocularZIP_AIC]
ocularBIC = [ocularPoisson_BIC, ocularNB_BIC, ocularNBP_BIC, ocularZIP_BIC]
ocularModelsDF = (pd.DataFrame({"model": ocularModels, "AIC": ocularAIC,
                                "BIC": ocularBIC})
                  .sort_values(["AIC", "BIC", "model"], ignore_index=True)
                  )
"""Poisson performed the best, having the lowest AIC & BIC values of the 4
models. Using the Poisson model results"""
ocularPoisson_fit = ocularPoisson.fit()
ocularPoisson_summary = ocularPoisson_fit.summary()
ocularMean = ocularData.groupby("safer")["numberOfChemicals"].mean()
"""p-value = 0.177, so not statistically significantly different. The 'safer'
products have 0.0613 CCs per product with this hazard trait, lower than the
'not safer' products which have 0.0767 CCs per product. Seems like this
difference isn't significant, however."""
# %%
"""Doing the same with Dermatotoxicity"""
dermData = hazardTraitCounts_Data4.query("HazardTrait == 'Dermatotoxicity'")

dermPoisson = sm.Poisson.from_formula(modelString_hazardTraits, dermData)
dermPoisson_AIC = AIC(2, -1223.6)
dermPoisson_BIC = BIC(2, -1223.6, 7152)

dermNB = sm.NegativeBinomial.from_formula(modelString_hazardTraits, dermData)
"""Model failed to fit, so disregard model."""

dermNBP = sm.NegativeBinomialP.from_formula(modelString_hazardTraits, dermData)
# Also failed to fit

dermZIP = sm.ZeroInflatedPoisson.from_formula(modelString_hazardTraits, dermData)
dermZIP_AIC = AIC(3, -1223.6)
dermZIP_BIC = BIC(3, -1223.6, 7152)

derm_ZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString_hazardTraits, dermData)
"""Also failed to fit. So only the Poisson models could be fitted. Let's
compare the 2 Poisson models."""
dermModels = ["Poisson", "zero-inflated Poisson"]
dermAIC = [dermPoisson_AIC, dermZIP_AIC]
dermBIC = [dermPoisson_BIC, dermZIP_BIC]
dermModelsDF = (pd.DataFrame({"model": dermModels, "AIC": dermAIC,
                              "BIC": dermBIC})
                .sort_values(["AIC", "BIC", "model"], ignore_index=True)
                )
"""Of the 2 Poisson models, regular Poisson performed best, having both lower
AIC & BIC than the zero-inflated model. Using it now to test for differences
between safer and not safer"""
dermPoisson_fit = dermPoisson.fit()
dermPoisson_summary = dermPoisson_fit.summary()
dermMean = dermData.groupby("safer")["numberOfChemicals"].mean()
"""The 'safer' products have 0.0582 CCs with this hazard trait per product,
more than the 'not safer' products which have 0.0391 CCs with this hazard trait
per product. This difference is statistically significant at p = 0.024"""
# %%
"""Carcinogenicity"""
carcData = hazardTraitCounts_Data4.query("HazardTrait == 'Carcinogenicity'")

carcPoisson = sm.Poisson.from_formula(modelString_hazardTraits, carcData)
carcPoisson_AIC = AIC(2, -6197)
carcPoisson_BIC = BIC(2, -6197, 7152)

carcNB = sm.NegativeBinomial.from_formula(modelString_hazardTraits, carcData)
carcNB_AIC = AIC(3, -6197.1)
carcNB_BIC = BIC(3, -6197.1, 7152)

carcNBP = sm.NegativeBinomialP.from_formula(modelString_hazardTraits, carcData)
"""Model was fitted, but it failed to calculate a log-likelihood, so I can't
calculate an AIC or BIC and compare it to other models. Disregard model."""

carcZIP = sm.ZeroInflatedPoisson.from_formula(modelString_hazardTraits, carcData)
carcZIP_AIC = AIC(3, -6197)
carcZIP_BIC = BIC(3, -6197, 7152)

carc_ZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString_hazardTraits, carcData)
"""Model failed to fit. So the models that fitted and that I can actually
compare are both Poisson models and the regular negative binomial model."""
carcModels = ["Poisson", "Negative Binomial", "zero-inflated Poisson"]
carcAIC = [carcPoisson_AIC, carcNB_AIC, carcZIP_AIC]
carcBIC = [carcPoisson_BIC, carcNB_BIC, carcZIP_BIC]
carcModelsDF = (pd.DataFrame({"model": carcModels, "AIC": carcAIC,
                              "BIC": carcBIC})
                .sort_values(["AIC", "BIC", "model"], ignore_index=True)
                )
"""The regular Poisson model performs best compared to the other 2 models.
Using it to test the difference between safer and not safer products for
carcinogens."""
carcPoisson_fit = carcPoisson.fit()
carcPoisson_summary = carcPoisson_fit.summary()
carcMean = carcData.groupby("safer")["numberOfChemicals"].mean()
"""Tiny p-value, p-value < 0.0001. The 'not safer' products have 0.512 CCs
with this hazard trait per product, in comparison to the 'safer' products which
have 0.0456 CCs with this hazard trait per product. And this difference is
statistically significant it looks like."""
# %%
"""Nephrotoxicity"""
kidneyData = hazardTraitCounts_Data4.query("HazardTrait == 'Nephrotoxicity and Other Toxicity to the Urinary System'")

kidneyPoisson = sm.Poisson.from_formula(modelString_hazardTraits, kidneyData)
kidneyPoisson_AIC = AIC(2, -1055.7)
kidneyPoisson_BIC = BIC(2, -1055.7, 7152)

kidneyNB = sm.NegativeBinomial.from_formula(modelString_hazardTraits, kidneyData)
# Failed to fit. Disregard.

kidneyNBP = sm.NegativeBinomialP.from_formula(modelString_hazardTraits, kidneyData)
# Also failed to fit.

kidneyZIP = sm.ZeroInflatedPoisson.from_formula(modelString_hazardTraits, kidneyData)
kidneyZIP_AIC = AIC(3, -1055.8)
kidneyZIP_BIC = BIC(3, -1055.8, 7152)

kidney_ZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString_hazardTraits, kidneyData)
# Failed to fit.

"""Only the 2 Poisson models could be fitted. Time to compare them to each
other"""
kidneyModels = ["Poisson", "zero-inflated Poisson"]
kidneyAIC = [kidneyPoisson_AIC, kidneyZIP_AIC]
kidneyBIC = [kidneyPoisson_BIC, kidneyZIP_BIC]
kidneyModelsDF = (pd.DataFrame({"model": kidneyModels, "AIC": kidneyAIC,
                              "BIC": kidneyBIC})
                  .sort_values(["AIC", "BIC", "model"], ignore_index=True)
                  )
"""Poisson has the lowest AIC & BIC values, so it wins over its zero-inflated
version."""
kidneyPoisson_fit = kidneyPoisson.fit()
kidneyPoisson_summary = kidneyPoisson_fit.summary()
kidneyMean = kidneyData.groupby("safer")["numberOfChemicals"].mean()
"""The 'not safer' products have a mean of 0.0373 CCs with this hazard trait
per product, higher than the 'safer' products at a mean of 0.00314 CCs with
this hazard trait per product. This difference is statistically significant at
p < 0.0001"""
# %%
"""Developmental toxicity"""
developmentalData = hazardTraitCounts_Data4.query("HazardTrait == 'Developmental Toxicity'")

developmentalPoisson = sm.Poisson.from_formula(modelString_hazardTraits, kidneyData)
developmentalPoisson_AIC = AIC(2, -1055.7)
developmentalPoisson_BIC = BIC(2, -1055.7, 7152)

developmentalNB = sm.NegativeBinomial.from_formula(modelString_hazardTraits, kidneyData)
"""Fitted I think? Like it calculated coefficients and also other values that
I can use for model comparison, but its parameters don't have anything like
p-values or confidence intervals. Ignore."""

developmentalNBP = sm.NegativeBinomialP.from_formula(modelString_hazardTraits, kidneyData)
"""Didn't fit properly. Like the negative binomial model, its parameters don't
have anything like p-values or confidence intervals. However, it also does not
have a log-likelihood, so I can't calculate BIC. Ignore."""

developmentalZIP = sm.ZeroInflatedPoisson.from_formula(modelString_hazardTraits, kidneyData)
developmentalZIP_AIC = AIC(3, -1055.8)
developmentalZIP_BIC = BIC(3, -1055.8, 7152)

developmentalZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString_hazardTraits, kidneyData)
"""Didn't fit properly again. Ignore. So I can only use the Poisson and its
zero-inflated version. Let's compare these 2 models, then."""
developmentalModels = ["Poisson", "zero-inflated Poisson"]
developmentalAIC = [developmentalPoisson_AIC, developmentalZIP_AIC]
developmentalBIC = [developmentalPoisson_BIC, developmentalZIP_BIC]
developmentalModelsDF = (pd.DataFrame({"model": developmentalModels,
                                       "AIC": developmentalAIC,
                                       "BIC": developmentalBIC})
                         .sort_values(["AIC", "BIC", "model"],
                                      ignore_index=True)
                         )
"""Regular Poisson model fits best with the lowest AIC & BIC values. Using it
to test for differences between safer & not safer products."""
developmentalPoisson_fit = developmentalPoisson.fit()
developmentalPoisson_summary = developmentalPoisson_fit.summary()
developmentalMean = developmentalData.groupby("safer")["numberOfChemicals"].mean()
"""The 'not safer' products have 0.0427 CCs with this hazard trait per product,
which is statistically significantly higher (p < 0.001) than the 'safer'
products which have 0.00314 CCs with this hazard trait per product."""
# %%
"""And, last but not least, 'Hazard Trait Undefined'"""
htUndefinedData = hazardTraitCounts_Data4.query("HazardTrait == 'Hazard Trait Undefined'")

htUndefinedPoisson = sm.Poisson.from_formula(modelString_hazardTraits, kidneyData)
htUndefinedPoisson_AIC = AIC(2, -1055.7)
htUndefinedPoisson_BIC = BIC(2, -1055.7, 7152)

htUndefinedNB = sm.NegativeBinomial.from_formula(modelString_hazardTraits, kidneyData)
# Didn't fit. Ignore.

htUndefinedNBP = sm.NegativeBinomialP.from_formula(modelString_hazardTraits, kidneyData)
# Didn't fit. Ignore.

htUndefinedZIP = sm.ZeroInflatedPoisson.from_formula(modelString_hazardTraits, kidneyData)
htUndefinedZIP_AIC = AIC(3, -1055.8)
htUndefinedZIP_BIC = BIC(3, -1055.8, 7152)

htUndefinedZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString_hazardTraits, kidneyData)
# Didn't fit. Ignore.

"""Again, only the 2 Poisson models could be fitted. Comparing them to each
other now."""
htUndefinedModels = ["Poisson", "zero-inflated Poisson"]
htUndefinedAIC = [htUndefinedPoisson_AIC, htUndefinedZIP_AIC]
htUndefinedBIC = [htUndefinedPoisson_BIC, htUndefinedZIP_BIC]
htUndefinedModelsDF = (pd.DataFrame({"model": developmentalModels,
                                     "AIC": developmentalAIC,
                                     "BIC": developmentalBIC})
                       .sort_values(["AIC", "BIC", "model"], ignore_index=True)
                       )
"""And, the regular Poisson model is the winner of the 2 Poisson models, again.
"""
htUndefinedPoisson_fit = htUndefinedPoisson.fit()
htUndefinedPoisson_summary = htUndefinedPoisson_fit.summary()
htUndefinedMean = htUndefinedData.groupby("safer")["numberOfChemicals"].mean()
"""The 'not safer' products contain a mean of 0.306 CCs with this hazard trait
per product, statistically significantly higher (p < 0.001) than 'safer'
products which contain a mean of 0.0189 CCs with this hazard trait per product.
"""
# %%
hazardTraitsInCommon_Mean = (hazardTraitCounts_Data4.groupby(["HazardTrait", "safer"])["numberOfChemicals"].mean()
                             .reset_index(drop=False)
                             .sort_values(["HazardTrait", "safer"], ascending=True)
                             )
hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Carcinogenicity", "model"] = "Poisson"
hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Carcinogenicity", "significance"] = "p-value < 0.001"

hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Dermatotoxicity", "model"] = "Poisson"
hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Dermatotoxicity", "significance"] = "p-value < 0.05"

hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Developmental Toxicity", "model"] = "Poisson"
hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Developmental Toxicity", "significance"] = "p-value < 0.001"

hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Hazard Trait Undefined", "model"] = "Poisson"
hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Hazard Trait Undefined", "significance"] = "p-value < 0.001"

hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Nephrotoxicity and Other Toxicity to the Urinary System", "model"] = "Poisson"
hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Nephrotoxicity and Other Toxicity to the Urinary System", "significance"] = "p-value < 0.001"

hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Ocular Toxicity", "model"] = "Poisson"
hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Ocular Toxicity", "significance"] = "p-value = 0.177"

hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Respiratory Toxicity", "model"] = "zero-inflated Poisson"
hazardTraitsInCommon_Mean.loc[hazardTraitsInCommon_Mean.HazardTrait == "Respiratory Toxicity", "significance"] = "p-value < 0.05"

hazardTraitsInCommon_Mean = (hazardTraitsInCommon_Mean.pivot(index=["HazardTrait", "model", "significance"], columns="safer", values="numberOfChemicals")
                             .reset_index()
                             .rename(columns={"No": "notSafer_CCsPerProduct", "Yes": "safer_CCsPerProduct"})
                             .sort_values(["notSafer_CCsPerProduct"], ascending=False)
                             )

"""Let's aggregate all the CCs that are in common so that we do a comparison
where the dependent variable is the number of CCs in any of the 7 common hazard
traits per product."""
commonHazardTraitsRaw = dataWithCCs.query("HazardTrait == @hazardTraitsBothList")
commonHazardTraitsData = (commonHazardTraitsRaw.groupby("productID")["DTXSID"].nunique()
                          .reset_index()
                          .rename(columns={"DTXSID": "numberOfCCs"})
                          .merge(dataCCcombined, "right", "productID")
                          .filter(["productID", "safer", "numberOfCCs"])
                          .drop_duplicates()
                          .astype({"safer": "category"})
                          )
commonHazardTraitsData.loc[commonHazardTraitsData.numberOfCCs.isna(), "numberOfCCs"] = 0

commonPoisson = sm.Poisson.from_formula(modelString, commonHazardTraitsData)
commonPoisson_AIC = AIC(2, -9077.7)
commonPoisson_BIC = BIC(2, -9077.7, 7152)

commonNB = sm.NegativeBinomial.from_formula(modelString, commonHazardTraitsData)
commonNB_AIC = AIC(3, -8803.2)
commonNB_BIC = BIC(3, -8803.2, 7152)

commonNBP = sm.NegativeBinomialP.from_formula(modelString, commonHazardTraitsData)
commonNBP_AIC = AIC(3, -8803.2)
commonNBP_BIC = BIC(3, -8803.2, 7152)

commonZIP = sm.ZeroInflatedPoisson.from_formula(modelString, commonHazardTraitsData)
commonZIP_AIC = AIC(3, -8862.4)
commonZIP_BIC = BIC(3, -8862.4, 7152)

commonZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString, commonHazardTraitsData)
commonZNBP_AIC = AIC(4, -8803.1)
commonZNBP_BIC = BIC(4, -8803.1, 7152)

commonModels = ["Poisson", "Negative Binomial",
                "Generalized Negative Binomial", "zero-inflated Poisson",
                "zero-inflated Generalized Negative Binomial"]
commonAIC = [commonPoisson_AIC, commonNB_AIC, commonNBP_AIC, commonZIP_AIC,
             commonZNBP_AIC]
commonBIC = [commonPoisson_BIC, commonNB_BIC, commonNBP_BIC, commonZIP_BIC,
             commonZNBP_BIC]
commonModelsDF = (pd.DataFrame({"model": commonModels, "AIC": commonAIC,
                                "BIC": commonBIC})
                  .sort_values(["AIC", "BIC", "model"], ignore_index=True)
                  )
"""The Negative Binomial and its Generalized version performs the best in
comparison to the other 3 models."""
commonNB_fit = commonNB.fit()
commonNB_summary = commonNB_fit.summary()

commonNBP_fit = commonNBP.fit()
commonNBP_summary = commonNBP_fit.summary()
commonMean = (commonHazardTraitsData.groupby("safer")["numberOfCCs"].mean()
              .reset_index()
              )
commonMean["HazardTrait"] = "7 hazard traits in common"
commonMean["model"] = "Negative Binomial; Generalized Negative Binomial"
commonMean["significance"] = "p-value < 0.001"
commonMean = (commonMean.pivot(index=["HazardTrait", "model", "significance"], columns="safer", values="numberOfCCs")
              .reset_index()
              .rename(columns={"No": "notSafer_CCsPerProduct", "Yes": "safer_CCsPerProduct"})
              )
hazardTraitsInCommon_Mean = pd.concat([hazardTraitsInCommon_Mean, commonMean], ignore_index=True)

# %%
"""The not safer products also contain 8 other hazard traits unique to
themselves. I want to see if the number of CCs that contain any of these 8
hazard traits per 'not safer' product also tends to be higher than the number
of CCs per 'safer' product."""
notSaferUniqueHazardTraitsRaw = dataWithCCs.loc[(dataWithCCs.HazardTrait.isin(hazardTraitsOnlyInNotSafer) & (dataWithCCs.safer == "No")) | (dataWithCCs.safer == "Yes")]
notSaferUniqueAggregatedData = (notSaferUniqueHazardTraitsRaw.groupby("productID")["DTXSID"].nunique()
                                .reset_index()
                                .rename(columns={"DTXSID": "numberOfCCs"})
                                .merge(dataCCcombined, "right", "productID")
                                .filter(["productID", "safer", "numberOfCCs"])
                                .drop_duplicates()
                                .astype({"safer": "category"})
                                )
notSaferUniqueAggregatedData.loc[notSaferUniqueAggregatedData.numberOfCCs.isna(), "numberOfCCs"] = 0

uniquePoisson = sm.Poisson.from_formula(modelString, notSaferUniqueAggregatedData)
uniquePoisson_AIC = AIC(2, -4068.9)
uniquePoisson_BIC = BIC(2, -4068.9, 7152)

uniqueNB = sm.NegativeBinomial.from_formula(modelString, notSaferUniqueAggregatedData)
uniqueNB_AIC = AIC(3, -4017.4)
uniqueNB_BIC = BIC(3, -4017.4, 7152)

uniqueNBP = sm.NegativeBinomialP.from_formula(modelString, notSaferUniqueAggregatedData)
uniqueNBP_AIC = AIC(3, -4017.4)
uniqueNBP_BIC = BIC(3, -4017.4, 7152)


uniqueZIP = sm.ZeroInflatedPoisson.from_formula(modelString, notSaferUniqueAggregatedData)
uniqueZIP_AIC = AIC(3, -3998.1)
uniqueZIP_BIC = BIC(3, -3998.1, 7152)

uniqueZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString, notSaferUniqueAggregatedData)
# Didn't fit. Disregard.

"""Both Poisson models, the regular negative binomial model, and its
generalized version, all could be fitted. Time to compare these 4 models."""
uniqueModels = ["Poisson", "Negative Binomial",
                "Generalized Negative Binomial", "zero-inflated Poisson"]
uniqueAIC = [uniquePoisson_AIC, uniqueNB_AIC, uniqueNBP_AIC, uniqueZIP_AIC]
uniqueBIC = [uniquePoisson_BIC, uniqueNB_BIC, uniqueNBP_BIC, uniqueZIP_BIC]
uniqueModelsDF = (pd.DataFrame({"model": uniqueModels, "AIC": uniqueAIC,
                                "BIC": uniqueBIC})
                  .sort_values(["AIC", "BIC", "model"], ignore_index=True)
                  )

"""Zero-inflated Poisson works best out of these 4 models."""
uniqueZIP_fit = uniqueZIP.fit()
uniqueZIP_summary = uniqueZIP_fit.summary()
uniqueMean = (notSaferUniqueAggregatedData.groupby("safer")["numberOfCCs"].mean()
              .reset_index()
              )
"""So 'not safer' products have a mean of 0.221 CCs per product, and these are
CCs with hazard traits unique to the 'not safer' products. This is
statistically significantly higher (p < 0.001) than the mean number of CCs per
product for 'safer' products (0.129). So even when you look at hazard traits
unique to the 'not safer' products, these products still have higher numbers of
these CCs than the 'safer' products."""
# %%
"""I guess I'll repeat all this analysis with a couple product categories. I'll
look at the product category with the highest number of safer products, since
this category likely would have enough of the smaller group to allow for high
enough power in hypothesis testing."""
categoriesCount = (dataCCcombined.groupby(["safer", "Product Category"])["productID"].nunique()
                   .reset_index()
                   .rename(columns={"productID": "numberOfProducts"})
                   .sort_values(["safer", "numberOfProducts"], ascending=False)
                   )
"""Worth noting that there is a certain level of mismatch in product categories
between the safer and 'not safer' products. There are 2,480 Makeup products in
the 'not safer' group, and Makeup has the highest number of products in this
group while there are only 27 'safer' Makeup products.

Let's look at Bodycare first, then skin care and haircare if I have time
"""
bodycareProducts = dataCCcombined.loc[dataCCcombined["Product Category"].str.contains("Bodycare")]
bodycareHazardsDF = (bodycareProducts.groupby(["safer", "HazardTrait"])["DTXSID"].nunique()
                     .reset_index()
                     .drop(columns=["DTXSID"])
                     )
bodycareCommonHazards = bodycareHazardsDF.loc[bodycareHazardsDF.safer == "Yes", "HazardTrait"].tolist()
bodycareCCcount = (bodycareProducts.groupby(["productID", "CCstatus"])["DTXSID"].nunique()
                   .reset_index()
                   .query("CCstatus == 'CC'")
                   .rename(columns={"DTXSID": "numberOfCCs"})
                   .drop(columns=["CCstatus"])
                   .merge(bodycareProducts, "right", "productID")
                   .filter(["productID", "numberOfCCs", "safer"])
                   .astype({"safer": "category"})
                   .drop_duplicates()
                   )
bodycareCCcount.loc[bodycareCCcount.numberOfCCs.isna(), "numberOfCCs"] = 0

bodycareOverallPoisson = sm.Poisson.from_formula(modelString, bodycareCCcount)
bodycareOverallPoisson_AIC = AIC(2, -189.48)
bodycareOverallPoisson_BIC = BIC(2, -189.48, 1091)

bodycareOverallNB = sm.NegativeBinomial.from_formula(modelString, bodycareCCcount)
bodycareOverallNB_AIC = AIC(3, -174.24)
bodycareOverallNB_BIC = BIC(3, -174.24, 1091)

bodycareOverallNBP = sm.NegativeBinomialP.from_formula(modelString, bodycareCCcount)
bodycareOverallNBP_AIC = AIC(3, -174.24)
bodycareOverallNBP_BIC = BIC(3, -174.24, 1091)

bodycareOverallZIP = sm.ZeroInflatedPoisson.from_formula(modelString, bodycareCCcount)
bodycareOverallZIP_AIC = AIC(3, -175.20)
bodycareOverallZIP_BIC = BIC(3, -175.20, 1091)

bodycareOverallZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString, bodycareCCcount)
bodycareOverallZNBP_AIC = AIC(4, -174.25)
bodycareOverallZNBP_BIC = BIC(4, -174.25, 1091)

bodycareOverallModels = ["Poisson", "Negative Binomial",
                         "Generalized Negative Binomial",
                         "zero-inflated Poisson",
                         "zero-inflated Generalized Negative Binomial"]
bodycareOveralAIC = [bodycareOverallPoisson_AIC, bodycareOverallNB_AIC,
                     bodycareOverallNBP_AIC, bodycareOverallZIP_AIC,
                     bodycareOverallZNBP_AIC]
bodycareOveralBIC = [bodycareOverallPoisson_BIC, bodycareOverallNB_BIC,
                     bodycareOverallNBP_BIC, bodycareOverallZIP_BIC,
                     bodycareOverallZNBP_BIC]
bodycareOverallModelsDF = (pd.DataFrame({"model": bodycareOverallModels,
                                         "AIC": bodycareOveralAIC,
                                         "BIC": bodycareOveralBIC})
                           .sort_values(["AIC", "BIC", "model"],
                                        ignore_index=True)
                           )

"""The 2 best performing models are the Negative Binomial model and its
generalized version, with both models performing equally well."""
bodycareOverallNB_fit = bodycareOverallNB.fit()
bodycareOverallNB_summary = bodycareOverallNB_fit.summary()

bodycareOverallNBP_fit = bodycareOverallNBP.fit()
bodycareOverallNBP_summary = bodycareOverallNBP_fit.summary()

bodycareOverallMean = (bodycareCCcount.groupby("safer")["numberOfCCs"].mean()
                       .reset_index()
                       )
"""The 'not safer' products contain 0.0478 CCs per product, statistically
significantly higher (p = 0.043 for both models) than the 'safer' bodycare
products which contain 0.0129 CCs per product. This is still a fairly low
number of CCs per product, though. Is it even worth it to analyze the hazard
traits for this category?"""
# %%
bodycareProducts_commonHazards = (bodycareProducts.query("HazardTrait == @bodycareCommonHazards")
                                  .rename(columns={"HazardTrait": "commonHazardTrait"})
                                  .groupby(["productID", "commonHazardTrait"])["DTXSID"].nunique()
                                  .reset_index()
                                  .rename(columns={"DTXSID": "numberOfChemicals"})
                                  .merge(bodycareProducts, "right", "productID")
                                  .filter(["productID", "commonHazardTrait", "safer", "numberOfChemicals"])
                                  .rename(columns={"commonHazardTrait": "HazardTrait"})
                                  .drop_duplicates()
                                  )

bodycareProductsList = bodycareProducts.productID.drop_duplicates().tolist()
bodycareFillingZeros = []
bodycareCommonHazardsSet = set(bodycareCommonHazards)
for product in bodycareProductsList:
    productDF = bodycareProducts_commonHazards.loc[bodycareProducts_commonHazards.productID == product]
    productHazards = set(productDF.HazardTrait.tolist())
    notInProduct = bodycareCommonHazardsSet - productHazards
    if len(notInProduct) >= 1:
        for hazard in notInProduct:
            bodycareFillingZeros.append([product, hazard, 0])
bodycareFillingZerosDF = pd.DataFrame(bodycareFillingZeros, columns=["productID", "HazardTrait", "numberOfChemicals"])
bodycareProducts_commonHazards = (pd.concat([bodycareProducts_commonHazards, bodycareFillingZerosDF],
                                            ignore_index=True)
                                  .sort_values(["productID", "safer"], ascending=True)
                                  )
bodycareProducts_commonHazards.safer = (bodycareProducts_commonHazards.safer.ffill()
                                        .astype("category")
                                        )
bodycareProducts_commonHazards = bodycareProducts_commonHazards.query("HazardTrait.notna()")

# %%
"""Testing whether 'safer' body care products and 'not safer' body care
products differ in the number of carcinogens per product."""

bodycareCarc = bodycareProducts_commonHazards.query("HazardTrait == 'Carcinogenicity'")

bodycareCarcPoisson = sm.Poisson.from_formula(modelString_hazardTraits, bodycareCarc)
bodycareCarcPoisson_AIC = AIC(2, -83.827)
bodycareCarcPoisson_BIC = BIC(2, -83.827, 1091)

bodycareCarcNB = sm.NegativeBinomial.from_formula(modelString_hazardTraits, bodycareCarc)
bodycareCarcNB_AIC = AIC(3, -82.573)
bodycareCarcNB_BIC = BIC(3, -82.573, 1091)

bodycareCarcNBP = sm.NegativeBinomialP.from_formula(modelString_hazardTraits, bodycareCarc)
bodycareCarcNBP_AIC = AIC(3, -82.573)
bodycareCarcNBP_BIC = BIC(3, -82.573, 1091)

bodycareCarcZIP = sm.ZeroInflatedPoisson.from_formula(modelString_hazardTraits, bodycareCarc)
bodycareCarcZIP_AIC = AIC(3, -82.529)
bodycareCarcZIP_BIC = BIC(3, -82.529, 1091)

bodycareCarcZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString_hazardTraits, bodycareCarc)
bodycareCarcZNBP_AIC = AIC(4, -82.572)
bodycareCarcZNBP_BIC = BIC(4, -82.572, 1091)

bodycareCarcModels = ["Poisson", "Negative Binomial",
                      "Generalized Negative Binomial", "zero-inflated Poisson",
                      "zero-inflated Generalized Negative Binomial"]
bodycareCarcAIC = [bodycareCarcPoisson_AIC, bodycareCarcNB_AIC,
                   bodycareCarcNBP_AIC, bodycareCarcZIP_AIC,
                   bodycareCarcZNBP_AIC]
bodycareCarcBIC = [bodycareCarcPoisson_BIC, bodycareCarcNB_BIC,
                   bodycareCarcNBP_BIC, bodycareCarcZIP_BIC,
                   bodycareCarcZNBP_BIC]
bodycareCarcModelsDF = (pd.DataFrame({"model": bodycareCarcModels,
                                      "AIC": bodycareCarcAIC,
                                      "BIC": bodycareCarcBIC})
                        .sort_values(["AIC", "BIC", "model"],
                                     ignore_index=True)
                        )

"""Zero-inflated Poisson fits the best."""
bodycareCarcZIP_fit = bodycareCarcZIP.fit()
bodycareCarcZIP_summary = bodycareCarcZIP_fit.summary()
bodycareCarcMean = (bodycareCarc.groupby("safer")["numberOfChemicals"].mean()
                    .reset_index()
                    )
"""'not safer' body care products have 0.0163 carcinogens per product in
comparison to 'safer' body care products which have 0.00858 carcinogens per
product. This difference was not significant (p = 0.413)"""
# %%
"""Testing whether 'safer' body care products and 'not safer' body care
products differ in the number of CCs with the hazard trait
'Hazard Trait Undefined' per product"""
bodycareUndefined = bodycareProducts_commonHazards.query("HazardTrait == 'Hazard Trait Undefined'")

bodycareUndefinedPoisson = sm.Poisson.from_formula(modelString_hazardTraits, bodycareUndefined)
bodycareUndefinedPoisson_AIC = AIC(2, -44.020)
bodycareUndefinedPoisson_BIC = BIC(2, -44.020, 1091)

bodycareUndefinedNB = sm.NegativeBinomial.from_formula(modelString_hazardTraits, bodycareUndefined)
# No log-likelihood. Did not fit properly.

bodycareUndefinedNBP = sm.NegativeBinomialP.from_formula(modelString_hazardTraits, bodycareUndefined)
bodycareUndefinedNBP_AIC = AIC(3, -35.988)
bodycareUndefinedNBP_BIC = BIC(3, -35.988, 1091)

bodycareUndefinedZIP = sm.ZeroInflatedPoisson.from_formula(modelString_hazardTraits, bodycareUndefined)
bodycareUndefinedZIP_AIC = AIC(3, -36.131)
bodycareUndefinedZIP_BIC = BIC(3, -36.131, 1091)

bodycareUndefinedZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(modelString_hazardTraits, bodycareUndefined)
bodycareUndefinedZNBP_AIC = AIC(4, -35.965)
bodycareUndefinedZNBP_BIC = BIC(4, -35.965, 1091)

"""I mean, all of these models show a pretty large p-value (p > 0.05), so it
seems like this hazard trait value doesn't really explain the difference in
the number of CCs per product between 'safer' and 'not safer' body care
products."""
# %%
"""I'm going to do a comparison between different product categories to see
the number of CCs per product between them. I will narrow down to product
categories with at least 30 products so that I can get high enough power, then
do an overall test using the 5 models to see if, overall, there is a difference
between product categories. I will select a model from this step and then use
this same model to do pairwise comparisons
"""
categoriesCount = (dataCCcombined.groupby("Product Category")["productID"].nunique()
                   .reset_index()
                   .rename(columns={"productID": "numberOfProducts"})
                   )
categoryDataset = (dataCCcombined.copy()
                   .merge(categoriesCount, "inner", "Product Category")
                   .rename(columns={"Product Category": "productCategory"})
                   .query("numberOfProducts >= 30")
                   .filter(["productID", "productCategory", "safer", "DTXSID", "HazardTrait", "CCstatus"])
                   .drop_duplicates()
                   )

# Let's filter out 'Hazard Trait Undefined' since it doesn't really mean much
categoryDataset.loc[categoryDataset.HazardTrait == "Hazard Trait Undefined", "CCstatus"] = "Hazard Trait Undefined"
categoryCCcount = (categoryDataset.groupby(["productID", "CCstatus"])["DTXSID"].nunique()
                   .reset_index()
                   .rename(columns={"DTXSID": "numberOfCCs"})
                   .query("CCstatus == 'CC'")
                   .drop(columns=["CCstatus"])
                   .drop_duplicates()
                   .merge(categoryDataset, "right", "productID")
                   .filter(["productID", "productCategory", "safer", "numberOfCCs"])
                   .drop_duplicates()
                   .astype({"productCategory": "category", "safer": "category"})
                   )
categoryCCcount.loc[categoryCCcount.numberOfCCs.isna(), "numberOfCCs"] = 0
categoryFormula = "numberOfCCs ~ C(productCategory)"

# And now, doing an overall test using 5 models and then comparing each model
categoryCCcountPoisson = sm.Poisson.from_formula(categoryFormula, categoryCCcount)
categoryCCcountPoisson_AIC = AIC(2, -44.020)
categoryCCcountPoisson_BIC = BIC(2, -44.020, 1091)

categoryCCcountNB = sm.NegativeBinomial.from_formula(categoryFormula, categoryCCcount)


categoryCCcountNBP = sm.NegativeBinomialP.from_formula(categoryFormula, categoryCCcount)
categoryCCcountNBP_AIC = AIC(3, -35.988)
categoryCCcountNBP_BIC = BIC(3, -35.988, 1091)

categoryCCcountZIP = sm.ZeroInflatedPoisson.from_formula(categoryFormula, categoryCCcount)
categoryCCcountZIP_AIC = AIC(3, -36.131)
categoryCCcountZIP_BIC = BIC(3, -36.131, 1091)

categoryCCcountZNBP = sm.ZeroInflatedNegativeBinomialP.from_formula(categoryFormula, categoryCCcount)
categoryCCcountZNBP_AIC = AIC(4, -35.965)
categoryCCcountZNBP_BIC = BIC(4, -35.965, 1091)
"""Well. I'm gonna be honest, I have no idea how to interpret these results.
Honest to god."""

"""Let's do this instead. I'm gonna do a Kruskal-Wallis to test for whether
there is an overall difference between all groups, and then I will do post-hoc
tests using discrete regression with p-values corrected using the
Holm-Bonferroni method."""
makeup = categoryCCcount.loc[categoryCCcount.productCategory == "Makeup", "numberOfCCs"]
haircare = categoryCCcount.loc[categoryCCcount.productCategory == "Haircare", "numberOfCCs"]
bodycare = categoryCCcount.loc[categoryCCcount.productCategory == "Bodycare", "numberOfCCs"]
skincare = categoryCCcount.loc[categoryCCcount.productCategory == "Skin Care", "numberOfCCs"]
personalHygiene = categoryCCcount.loc[categoryCCcount.productCategory == "Personal Hygiene", "numberOfCCs"]
nail = categoryCCcount.loc[categoryCCcount.productCategory == "Nail Products", "numberOfCCs"]
babycare = categoryCCcount.loc[categoryCCcount.productCategory == "Babycare/Kidcare", "numberOfCCs"]
fragrance = categoryCCcount.loc[categoryCCcount.productCategory == "Fragrance", "numberOfCCs"]
hairAndBody = categoryCCcount.loc[categoryCCcount.productCategory == "Haircare, Bodycare", "numberOfCCs"]

categoryKruskal = stats.kruskal(makeup, haircare, bodycare, skincare, nail,
                                personalHygiene, babycare, fragrance,
                                hairAndBody)
numberOfCategories = len(categoryDataset.productCategory.drop_duplicates().tolist())
kruskalDegreesOfFreedom = numberOfCategories - 1
chi2pvalue = 1 - stats.chi2.cdf(categoryKruskal.statistic, kruskalDegreesOfFreedom)
"""Tiny ass p-value, so there is an overall difference in the number of CCs
per product between the different product categories. I'm gonna do post-hoc
tests using discrete regression.

The smallest p-value I could calculate comes from a chi-squared value of 94,
with a p-value of 1.11 * 10^-16. Any larger chi-squared value, and the only
p-value I could get is 0.0
"""

productCategories = categoryDataset.productCategory.drop_duplicates().tolist()
categoryPairs = list(itertools.combinations(productCategories, 2))
categoryPairs = pd.DataFrame(categoryPairs, columns=["category1", "category2"])
"""Ok. I have 36 combinations that I need to do a pairwise test on. If I fit 5
models to each of these 36 combinations, that means I would have to fit a total
of 180 models. I'm not doing that. I'll just pick only 1 model and run this
model on each of the 36 combinations, then.

I was going to use the Poisson model, but then I got a 'Singular matrix' error.
I switched to the zero-inflated Poisson but it didn't fit for quite a few of
these combinations. The fuck am I supposed to do now, Mann-Whitney U?

After thinking about this a bit more, if I have time, I'd do post-hoc tests
using the Kruskal-Wallis test corrected using a Holm-Bonferroni correction.
Then I would create a compact letter display using these post-hoc test results.
However, I simply do not have the time now, so we'll just have to settle with
this overall Kruskal-Wallis test.
"""
resultsTableRename = {0: "parameter", 1: "coef", 2: "std_err", 3: "z",
                      4: "pValue", 5: "leftTailCoefficient",
                      6: "rightTailCoefficient"}
for index, row in categoryPairs.iterrows():    
    categories = [row.category1, row.category2]
    data = categoryCCcount.copy()
    data = data.loc[data.productCategory.isin(categories)]
    # posthocModel = sm.Poisson.from_formula(categoryFormula, data)
    # modelFit = posthocModel.fit()
    # modelResults = modelFit.summary()
    # modelResultsTable = modelResults.tables[1]
    # modelResultsTableList = pd.read_html(modelResultsTable.as_html())
    # modelResultsDF = modelResultsTableList[0]
    # modelResultsDF = modelResultsDF.rename(columns=resultsTableRename)
    # modelResultsDF = modelResultsDF[1:]
    # pValueString = modelResultsDF.loc[modelResultsDF.parameter.str.contains("safer"), "pValue"].tolist()
    # print(pValueString)
    # pValue = float(pValueString)
    # categoryPairs.loc[index, "pValue"] = pValue
# %%
"""Starting the process of making figures. Gonna make histograms when I'm
comparing safer vs not safer products. Then I'm gonna make a bar chart for
comparing different product categories.

I'll make a figure with 3 subplots, each of which are histograms. This figure
will plot (1) CCs per product, (2) CCs with common hazard traits per product,
and (3) 'not safer' products and hazard traits unique to them vs 'safer'
products.

The histograms of overall CCs per product, I'll also make it into its own
separate figure to show how the data is distributed overall.

I'll make another figure with a couple subplots, each of which are histograms
plotting the number of CCs with a specific hazard trait. It'll have 7 subplots
for (1) Carcinogenicity, (2) Hazard Trait Undefined, (3) Respiratory Toxicity,
(4) Ocular Toxicity, (5) Developmental Toxicity, (6) Dermatotoxicity, and (7)
kidney toxicity.

I'll make a final figure that'll be a boxplot with 9 boxplots, 1 for each of
the 9 product categories that I performed the overall Kruskal-Wallis test on.

This chunk is for making the figure with 3 subplots.
"""
overallFigure = plt.figure("Overall figure", (20, 15))

bins = [0, 1, 2, 3, 4, 5, 6, 7]

allHazards = plt.subplot(2, 2, 1)
allHazards.hist(CCsPerProduct_Data.loc[CCsPerProduct_Data.safer == "Yes", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="safer")
allHazards.hist(CCsPerProduct_Data.loc[CCsPerProduct_Data.safer == "No", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="not safer")
allHazards.set_title("All hazard traits")
allHazards.set_xlabel("Number of CCs per product")
allHazards.set_ylabel("Frequency")
allHazards.annotate("p-value < 0.001", (6, 0.78))
allHazards.grid(visible=True)
allHazards.set_ybound(upper=0.9)
plt.legend()

commonHazards = plt.subplot(2, 2, 3)
commonHazards.hist(commonHazardTraitsData.loc[commonHazardTraitsData.safer == "Yes", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="safer")
commonHazards.hist(commonHazardTraitsData.loc[commonHazardTraitsData.safer == "No", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="not safer")
commonHazards.set_title("Hazard traits in common")
commonHazards.set_xlabel("Number of CCs per product")
commonHazards.set_ylabel("Frequency")
commonHazards.annotate("p-value < 0.001", (6, 0.78))
commonHazards.grid(visible=True)
commonHazards.set_ybound(upper=0.9)
plt.legend()

uniqueHazards = plt.subplot(2, 2, 4)
uniqueHazards.hist(notSaferUniqueAggregatedData.loc[notSaferUniqueAggregatedData.safer == "Yes", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="safer")
uniqueHazards.hist(notSaferUniqueAggregatedData.loc[notSaferUniqueAggregatedData.safer == "No", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="not safer")
uniqueHazards.set_title("Hazard traits unique to 'not safer' vs all hazard traits in 'safer'")
uniqueHazards.set_xlabel("Number of CCs per product")
uniqueHazards.set_ylabel("Frequency")
uniqueHazards.annotate("p-value < 0.001", (6, 0.78))
uniqueHazards.grid(visible=True)
uniqueHazards.set_ybound(upper=0.9)
plt.legend()

overallFigureSmall = plt.figure("Overall figure small", (15, 12))

allHazards2 = plt.subplot(1, 1, 1)
allHazards2.hist(CCsPerProduct_Data.loc[CCsPerProduct_Data.safer == "Yes", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="safer")
allHazards2.hist(CCsPerProduct_Data.loc[CCsPerProduct_Data.safer == "No", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="not safer")
allHazards2.set_xlabel("Number of CCs per product")
allHazards2.set_ylabel("Frequency")
allHazards2.grid(visible=True)
allHazards2.set_ybound(upper=0.9)
plt.legend()
# %%
"""Now to make the figure of the 7 hazard traits in common between safer and
not safer products. Let's make a plot with 8 subplots. The subplots are (1)
CCs containing any of the 7 hazard traits in common; (2) carcinogens; (3) CCs
with the value 'Hazard Trait Undefined'; (4) respiratory toxicants; (5) ocular
toxicants; (6) developmental toxicants; (7) dermatotoxicants; and (8) CCs that
are toxic to kidneys and/or the urinary system."""
commonHazardsFigure = plt.figure("Common hazards", (30, 15))

commonHazards2 = plt.subplot(2, 4, 1)
commonHazards2.hist(commonHazardTraitsData.loc[commonHazardTraitsData.safer == "Yes", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="safer")
commonHazards2.hist(commonHazardTraitsData.loc[commonHazardTraitsData.safer == "No", "numberOfCCs"], alpha=0.3, bins=bins, density=True, label="not safer")
commonHazards2.set_title("Hazard traits in common")
commonHazards2.set_xlabel("Number of CCs per product")
commonHazards2.set_ylabel("Frequency")
commonHazards2.annotate("p-value < 0.001", (5.5, 0.87))
commonHazards2.grid(visible=True)
commonHazards2.set_ybound(upper=1)
plt.legend()

carcinogens = plt.subplot(2, 4, 2)
carcinogens.hist(carcData.loc[carcData.safer == "Yes", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="safer")
carcinogens.hist(carcData.loc[carcData.safer == "No", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="not safer")
carcinogens.set_title("Carcinogenicity")
carcinogens.set_xlabel("Number of CCs per product")
carcinogens.set_ylabel("Frequency")
carcinogens.annotate("p-value < 0.001", (5.5, 0.87))
carcinogens.grid(visible=True)
carcinogens.set_ybound(upper=1)
plt.legend()

htUndefined = plt.subplot(2, 4, 3)
htUndefined.hist(htUndefinedData.loc[htUndefinedData.safer == "Yes", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="safer")
htUndefined.hist(htUndefinedData.loc[htUndefinedData.safer == "No", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="not safer")
htUndefined.set_title("Hazard Trait Undefined")
htUndefined.set_xlabel("Number of CCs per product")
htUndefined.set_ylabel("Frequency")
htUndefined.annotate("p-value < 0.001", (5.5, 0.87))
htUndefined.grid(visible=True)
htUndefined.set_ybound(upper=1)
plt.legend()

respiratory = plt.subplot(2, 4, 4)
respiratory.hist(respiratoryData.loc[respiratoryData.safer == "Yes", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="safer")
respiratory.hist(respiratoryData.loc[respiratoryData.safer == "No", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="not safer")
respiratory.set_title("Respiratory Toxicity")
respiratory.set_xlabel("Number of CCs per product")
respiratory.set_ylabel("Frequency")
respiratory.annotate("p-value < 0.05", (5.5, 0.87))
respiratory.grid(visible=True)
respiratory.set_ybound(upper=1)
plt.legend()

developmental = plt.subplot(2, 4, 5)
developmental.hist(developmentalData.loc[developmentalData.safer == "Yes", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="safer")
developmental.hist(developmentalData.loc[developmentalData.safer == "No", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="not safer")
developmental.set_title("Developmental Toxicity")
developmental.set_xlabel("Number of CCs per product")
developmental.set_ylabel("Frequency")
developmental.annotate("p-value < 0.001", (5.5, 0.87))
developmental.grid(visible=True)
developmental.set_ybound(upper=1)
plt.legend()

kidney = plt.subplot(2, 4, 6)
kidney.hist(kidneyData.loc[kidneyData.safer == "Yes", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="safer")
kidney.hist(kidneyData.loc[kidneyData.safer == "No", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="not safer")
kidney.set_title("Kidney & other urinary system toxicity")
kidney.set_xlabel("Number of CCs per product")
kidney.set_ylabel("Frequency")
kidney.annotate("p-value < 0.001", (5.5, 0.87))
kidney.grid(visible=True)
kidney.set_ybound(upper=1)
plt.legend()





skin = plt.subplot(2, 4, 7)
skin.hist(dermData.loc[dermData.safer == "Yes", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="safer")
skin.hist(dermData.loc[dermData.safer == "No", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="not safer")
skin.set_title("Dermatotoxicity")
skin.set_xlabel("Number of CCs per product")
skin.set_ylabel("Frequency")
skin.annotate("p-value < 0.05", (5.5, 0.87))
skin.grid(visible=True)
skin.set_ybound(upper=1)
plt.legend()


eye = plt.subplot(2, 4, 8)
eye.hist(ocularData.loc[ocularData.safer == "Yes", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="safer")
eye.hist(ocularData.loc[ocularData.safer == "No", "numberOfChemicals"], alpha=0.3, bins=bins, density=True, label="not safer")
eye.set_title("Ocular Toxicity")
eye.set_xlabel("Number of CCs per product")
eye.set_ylabel("Frequency")
eye.annotate("p-value = 0.177", (5.5, 0.87))
eye.grid(visible=True)
eye.set_ybound(upper=1)
plt.legend()

# %%
"""Now, to make boxplots comparing the different product categories to each
other."""
categoryCCsummary = (categoryCCcount.groupby("productCategory")["numberOfCCs"].describe()
                     .sort_values("mean", ascending=False)
                     .reset_index(drop=False)
                     )
categoriesToPlot = categoryCCsummary.productCategory.tolist()

categoriesFigure = plt.figure("Product categories CCs", (15, 15))
categoriesPlot = plt.subplot(1, 1, 1)
categoriesDataToPlot = []
for category in categoriesToPlot:
    categoriesDataToPlot.append(categoryCCcount.loc[categoryCCcount.productCategory == category, "numberOfCCs"].tolist())

categoriesPlot.boxplot(categoriesDataToPlot, whis=(25, 75), labels=categoriesToPlot, showfliers=False, showmeans=True)
categoriesPlot.set_ylabel("Number of CCs per product")
categoriesPlot.set_xlabel("Product categories")
categoriesPlot.annotate("p-value < 10^-15", (8.2, 4.05))
# %%
"""Now, to export the figures."""
overallFigureSmallPath = outputFolder/"Overall distributions.png"
if os.path.exists(overallFigureSmallPath) is False:
    overallFigureSmall.savefig(overallFigureSmallPath, dpi=400, bbox_inches="tight")
    plt.close("Overall figure small")

overallFigurePath = outputFolder/"All CCs comparisons.png"
if os.path.exists(overallFigurePath) is False:
    overallFigure.savefig(overallFigurePath, dpi=400, bbox_inches="tight")
    plt.close("Overall figure")

commonHazardsFigurePath = outputFolder/"Common hazard traits.png"
if os.path.exists(commonHazardsFigurePath) is False:
    commonHazardsFigure.savefig(commonHazardsFigurePath, dpi=400, bbox_inches="tight")
    plt.close("Common hazards")

categoriesPlotPath = outputFolder/"Product categories comparison.png"
if os.path.exists(categoriesPlotPath) is False:
    categoriesFigure.savefig(categoriesPlotPath, dpi=400, bbox_inches="tight")
    plt.close("Product categories CCs")
# %%
"""Let's also export a couple tables of results as Excel files."""
note = ["This file contains some summary statistics from my analysis of safer",
        "vs not safer products and from my comparison of different product",
        "categories. This analysis only looks at CCs in the BCPP product",
        "dataset, not other chemicals not on the CC list."]
readMe = pd.DataFrame({"Note": note})
overallResults = (CCsPerProduct_mean.copy()
                  .reset_index(drop=False)
                  )
overallResults["model"] = "zero-inflated Generalized Negative Binomial"
overallResults["significance"] = "p-value < 0.001"
overallResults["HazardTrait"] = "All hazard traits"
overallResults = (overallResults.pivot(["model", "significance", "HazardTrait"],
                                       columns="safer", values="numberOfCCs")
                  .reset_index(drop=False)
                  .rename(columns={"No": "notSafer_CCsPerProduct", "Yes": "safer_CCsPerProduct"})
                  )

uniqueMeanPivot = uniqueMean.copy()
uniqueMeanPivot["model"] = "zero-inflated Poisson"
uniqueMeanPivot["significance"] = "p-value < 0.001"
uniqueMeanPivot["HazardTrait"] = "Hazard traits unique to 'not safer' vs hazard traits in common"

uniqueMeanPivot = (uniqueMeanPivot.pivot(["model", "significance", "HazardTrait"],
                                         columns="safer", values="numberOfCCs")
                   .reset_index(drop=False)
                   .rename(columns={"No": "notSafer_CCsPerProduct", "Yes": "safer_CCsPerProduct"})
                   )

overallResults = pd.concat([overallResults, commonMean, uniqueMeanPivot], ignore_index=True)

resultsPath = outputFolder/"Safer vs not safer results.xlsx"
if os.path.exists(resultsPath) is False:
    with pd.ExcelWriter(resultsPath) as w:
        readMe.to_excel(w, "ReadMe", index=False)
        overallResults.to_excel(w, "Overall comparisons", index=False)
        hazardTraitsInCommon_Mean.to_excel(w, "Hazards in common", index=False)
        categoryCCsummary.to_excel(w, "Product categories", index=False)
