# -*- coding: utf-8 -*-
"""
Created on Wed Dec 24 15:05:53 2025

@author: BChung
DTSC entered into a contract with the Breast Cancer Prevention Partners (BCPP)
in which BCPP provides DTSC with a copy of a product database BCPP has on
products that are specifically marketed towards African-Americans. I was asked
to do some exploratory data analysis with this dataset.

This script is to assign product identifiers. There are 2 product datasets,
one that lists products for 2022 and one for 2024. I'm not sure if both
datasets share any products.
"""
import os
from pathlib import Path
import pandas as pd
from rapidfuzz import fuzz

repository = Path(os.getcwd())
projectFolder = Path(os.path.dirname(repository))
dataFolder = projectFolder/"Data"
inputFolder = dataFolder/"Input"
outputFolder = dataFolder/"Output"

dataPath = inputFolder/"CSC Black Beauty Product Database and Screening Results.xlsx"
datasets = pd.ExcelFile(dataPath)
# %%
sheetNames = datasets.sheet_names
"""It's, very weird that when you open up the file in Excel, you see only 6
sheets, but when I read it into Python using Pandas, I see 15 sheets."""
allProducts = pd.read_excel(datasets, "DONT USE - Copy of All Products")

"""Checking for columns that only contain nan values, will remove these columns
because they contain absolutely no data and so are meaningless. Also checking
to see if there are columns that only contain single values, since these values
might be too redundant.
"""
allProductsColumns = allProducts.columns.tolist()
nanColumns = []
singleValueColumns = []
for column in allProductsColumns:
    deduplicatedColumn = allProducts[column].drop_duplicates()
    columnNoNA = deduplicatedColumn.dropna()
    if columnNoNA.empty:
        nanColumns.append(column)
    elif deduplicatedColumn.shape[0] == 1:
        singleValueColumns.append(column)

"""So the list nanColumns and singleValueColumns each contain only a single
column. Let's drop both of these columns since they are meaningless"""
allProductsDrop = nanColumns + singleValueColumns
allProducts = (allProducts.drop(columns=allProductsDrop)
               .drop_duplicates()
               )

"""Renaming columns in the table of all products"""
allProductsOldCols = allProducts.columns.tolist()
allProductsColsRename = {allProductsOldCols[5]: "Breadcrumbs", allProductsOldCols[6]: "productCategory", allProductsOldCols[8]: "priceUSD", allProductsOldCols[9]: "priceAdmin", allProductsOldCols[11]: "productLabelURL", allProductsOldCols[14]: "barcodeUPC", allProductsOldCols[17]: "saferIn2022", allProductsOldCols[18]: "timeOfCollection", allProductsOldCols[29]: "forProfessionalSalonUse", allProductsOldCols[30]: "forMen", allProductsOldCols[31]: "copyOfTimeOfCollection"}
allProducts = allProducts.rename(columns=allProductsColsRename)

allProductsDrop2 = ["Price Range"] + [string for string in allProductsOldCols if "Under $" in string]
allProducts = allProducts.drop(columns=allProductsDrop2)

"""I manually compared 2 products by the brand Aja Naturals (1 named Lavender
Sugar Scrub and 1 named Baby Love) between the 2022 tab and the copy of all
products tab and I can see that there are some slight differences in field
values between these 2 tabs. The following fields differ slightly between these
2 tabs:

- product URL
- prices in USD
- breadcrumb
- ingredient list
- product category

Not entirely sure why there are these differences. Here's what I'll do. For the
Aja Naturals product with the product name of 'Oat Couture - Exfoliating Bar Soap',
I'll separate it out from the 2022 data and the all products data and treat it
later. For the rest of the products in the all products tab, I'll only keep the
fields for brand, brand ID, product name, product ID, ingredient list, price in USD, and
product description. I'll then merge it with the 2022 data based on brand and
product names. For products that are missing values for ingredient list, price in USD,
and product description, I'll fill them in with the data from the all products tab.
Otherwise, I will just keep the data from the 2022 data."""

allProductsKeep = ["Brand", "Brand Id", "Product name", "Product ID",
                   "priceUSD", "Ingredient list", "barcodeUPC",
                   "Product description", "saferIn2022"]
allProductsRename = {"Brand": "brand", "Brand Id": "brandID",
                     "Product name": "productName", "Product ID": "productID",
                     "Ingredient list": "ingredientList",
                     "Product description": "productDescription"}
allProducts = (allProducts.filter(allProductsKeep)
               .rename(columns=allProductsRename)
               )
# %%
"""Importing the 2022 data and processing it by
- adding product identifiers
- for fields that are missing values, fill them in with data from the all
products tab
"""
data2022 = pd.read_excel(datasets, "2022_Black Beauty Products")
ajaOatCouture = data2022.query("`Product name` == 'Oat Couture - Exfoliating Bar Soap'")
data2022 = data2022.query("(`Product name` != ['Oat Couture - Exfoliating Bar Soap', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1978, 92982.3]) & (`Product name`.notna())")

# Checking for unequal prices
unequalPrices2022 = data2022.query("priceUSD1 != priceUSD2")
"""Ok, so there are some products with different price values where priceUSD1
indicates a range of prices (likely based on product size) and priceUSD2 seems
to be the mean between the upper and lower end of this range. I'll just drop
priceUSD1"""
data2022 = (data2022.drop(columns=["priceUSD1", "Unnamed: 11", "Unnamed: 12"])
            .rename(columns={"priceUSD2": "priceUSD", "Brand": "brand",
                             "Product name": "productName"})
            )

# Adding only brand and product IDs
allProductsIDs = allProducts.filter(["brand", "brandID", "productName", "productID"])
data2022 = data2022.merge(allProductsIDs, "left", ["brand", "productName"],
                          indicator=True)
data2022strictMatch = (data2022.query("_merge == 'both'")
                       .drop(columns=["_merge"])
                       .drop_duplicates()
                       )
data2022unmatch = data2022.query("_merge == 'left_only'")

"""Ok so I matched by the brand name and product name and there are ~5k product
entries that are still unmatched. Let's do some fuzzy string matching then.
I'll match brands together first"""
allProductsBrands = allProducts.brand.drop_duplicates().tolist()
data2022brandsUnmatched = data2022unmatch.brand.drop_duplicates().tolist()


def fuzzyMatch1(list1, list2, fuzzyScore, cutoff=90):
    """
    Performs fuzzy string matching between elements in list1 and list2.
    Generates combinations of 2 elements each with 1 element from each list and
    then calculates a similarity score between these 2 elements. This specific
    function will then filter for combinations with a similarity score of at
    least a certain cutoff.

    Parameters
    ----------
    list1 : List of string
        First list of elements to be matched.
    list2 : List of string
        Second list of elements to be matched.
    fuzzyScore : fuzz method
        rapidfuzz.fuzz method.
    cutoff : numeric, optional
        The minimum similarity score a combination needs to have to be kept.
        The default is 90.

    Returns
    -------
    A Pandas dataframe with preliminarily matched elements.

    """
    pairs = []
    for element1 in list1:
        for element2 in list2:
            similarity = fuzzyScore(element1, element2)
            pair = [element1, element2, similarity]
            pairs.append(pair)
    df = pd.DataFrame(pairs, columns=["field1", "field2", "similarity"])
    df = df.loc[df.similarity >= cutoff]
    df = df.sort_values(["similarity", "field1"], ascending=[False, True],
                        ignore_index=True)
    return df


data2022brandsFuzzy = fuzzyMatch1(allProductsBrands, data2022brandsUnmatched, fuzz.ratio, 84.8)

data2022brandsFuzzy.loc[188:189, "match"] = False
data2022brandsFuzzy.loc[191:192, "match"] = False

data2022brandsFuzzy = (data2022brandsFuzzy.query("match != False")
                       .drop(columns=["match", "similarity"])
                       .rename(columns={"field1": "brandAllProducts", "field2": "brand2022dataset"})
                       )

"""Now that I matched brands together, I'm going to match by product names
next. I'm going to have product names of the all products dataset on the left
and product names of the 2022 product dataset on the right and then calculate
similarity scores for each pair."""
data2022fuzzy = (data2022brandsFuzzy.merge(allProductsIDs, "left", left_on="brandAllProducts", right_on="brand")
                 .drop(columns=["brandID", "productID", "brand"])
                 .rename(columns={"productName": "nameAllProducts"})
                 .merge(data2022unmatch, "left", left_on="brand2022dataset", right_on="brand")
                 .rename(columns={"productName": "name2022products"})
                 .drop(columns=["brand", "URL", "Breadcrumbs", "Product Category", "priceUSD", "Ingredient list", "Leading product photo URL", "Product description", "SAFER BLACK BEAUTY BRAND PRODUCT?", "brandID", "productID", "_merge"])
                 .drop_duplicates()
                 )
nameAllProducts = data2022fuzzy.nameAllProducts.tolist()
name2022products = data2022fuzzy.name2022products.tolist()
productNameScores = []
for i in range(len(nameAllProducts)):
    nameAll = nameAllProducts[i]
    name2022 = name2022products[i]
    score = fuzz.partial_ratio(nameAll, name2022)
    productNameScores.append(score)
data2022fuzzy2 = pd.DataFrame({"nameAllProducts": nameAllProducts, "name2022products": name2022products, "score": productNameScores})
data2022fuzzy = (data2022fuzzy.merge(data2022fuzzy2, "left", ["nameAllProducts", "name2022products"])
                 .query("score >= 82")
                 )
"""I think, at this point, I'm just not gonna match the product IDs up against
the datasets that BCPP meant for us to see. Doing that is more of a pain that I
expected with mismatches in product names between the datasets that BCPP meant
for us to see and the hidden dataset of all products."""
