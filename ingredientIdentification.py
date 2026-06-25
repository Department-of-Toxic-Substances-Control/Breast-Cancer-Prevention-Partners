# -*- coding: utf-8 -*-
"""
Created on Mon Jun 15 11:20:34 2026

@author: BChung

This script compiles together product and ingredient data after ingredient
identification via a combination of strict and fuzzy string matching with
CompTox and EU CosIng. The output will be an Excel file that serves as the
dataset that will be analyzed.
"""
import os
from pathlib import Path
import pandas as pd

repository = Path(os.getcwd())
repositoryFolder = Path(os.path.dirname(repository))
dataFolder = repositoryFolder/"Data"
inputFolder = dataFolder/"Input"
outputFolder = dataFolder/"Output"

"""The following files are to be imported in this script but not necessarily
in this chunk

Cleaned ingredient lists & ingredient names.xlsx
- In the Output folder
- 2022 products, 2024 products, Ingredient lists, List - ingredient combos,
Ingredients

Identified & unidentified after batch search.xlsx
- Output folder
- Identified CompTox, Identified CosIng, Unidentified

CosIng CAS RN batch search.xlsx
- Input folder
- Main Data

CompTox fuzzy.xlsx
- Output folder
- Fuzzy CompTox

CosIng fuzzy.xlsx
- Output folder
- Fuzzy CosIng

Fuzzy CosIng CAS RN batch search.csv
- Input folder
"""

"""I'll spend this chunk compiling the ingredients that have been identified
through either the CompTox batch search or fuzzy string matching with the
CompTox batch search results."""
strictMatchingPath = outputFolder/"Identified & unidentified after batch search.xlsx"
CompToxStrict = (pd.read_excel(strictMatchingPath, "Identified CompTox", dtype="string")
                 .drop(columns=["FOUND_BY"])
                 )
CompToxStrict["identified"] = CompToxStrict.ingredientName.str.upper()
CosIngStrict = (pd.read_excel(strictMatchingPath, "Identified CosIng", dtype="string")
                .rename(columns={"CASRN": "CosIngCASRN"})
                )
# CompToxStrict also essentially contains the CompTox batch search results
unidentifiedAfterStrict = pd.read_excel(strictMatchingPath, "Unidentified", dtype="string")
unidentifiedAfterStrict["unidentified"] = unidentifiedAfterStrict.ingredientName.str.upper()

CompToxFuzzyPath = outputFolder/"CompTox fuzzy.xlsx"
CompToxFuzzy = (pd.read_excel(CompToxFuzzyPath, "Fuzzy CompTox", dtype="string")
                .merge(CompToxStrict, "left", "identified")
                .drop(columns=["ingredientName"])
                .drop_duplicates()
                .merge(unidentifiedAfterStrict, "left", "unidentified")
                .drop(columns=["unidentified"])
                .drop_duplicates()
                )
CompToxStrict["identificationMethod"] = "CompTox strict matching"
CompToxFuzzy["identificationMethod"] = "CompTox fuzzy matching"
CompTox = (pd.concat([CompToxStrict, CompToxFuzzy], ignore_index=True)
           .drop_duplicates()
           )
CompTox = CompTox.loc[:, ["ingredientName", "identified", "DTXSID", "PREFERRED_NAME", "CASRN", "SMILES", "identificationMethod"]]
# %%
"""Now, compiling the ingredients that have been identified through strict and
fuzzy string matching with CosIng"""
CosIngStrict["identificationMethod"] = "CosIng strict matching"
CosIngFuzzyPath = outputFolder/"CosIng fuzzy.xlsx"
CosIngFuzzy = (pd.read_excel(CosIngFuzzyPath, "Fuzzy CosIng", dtype="string")
               .merge(unidentifiedAfterStrict, "left", "unidentified")
               .drop(columns=["unidentified"])
               .drop_duplicates()
               )
CosIngFuzzy["identificationMethod"] = "CosIng fuzzy matching"

CosIngStrictCASRNpath = inputFolder/"CosIng CAS RN batch search.xlsx"
CosIngStrictCASRN = pd.read_excel(CosIngStrictCASRNpath, "Main Data", dtype="string")
CosIngFuzzyCASRNpath = inputFolder/"Fuzzy CosIng CAS RN batch search.xlsx"
CosIngFuzzyCASRN = pd.read_excel(CosIngFuzzyCASRNpath, "Main Data", dtype="string")
CosIngCASRNbatch = (pd.concat([CosIngStrictCASRN, CosIngFuzzyCASRN], ignore_index=True)
                    .drop(columns=["FOUND_BY"])
                    .drop_duplicates()
                    .query("DTXSID.notna()")
                    .rename(columns={"INPUT": "CosIngCASRN"})
                    )

CosIng = (pd.concat([CosIngStrict, CosIngFuzzy], ignore_index=True)
          .merge(CosIngCASRNbatch, "left", "CosIngCASRN")
          .drop_duplicates()
          .rename(columns={"INCI": "identified"})
          )
# %%
"""Importing in the product data as well"""

identifiedIngredients = pd.concat([CompTox, CosIng], ignore_index=True)
identifiedIngredients.loc[identifiedIngredients.SMILES.str.isspace(), "SMILES"] = pd.NA

productDataPath = outputFolder/"Cleaned ingredient lists & ingredient names.xlsx"
products2022 = pd.read_excel(productDataPath, "2022 products",
                             dtype={"productID": "string", "Brand": "string", "URL": "string", "Product name": "string", "Breadcrumbs": "string", "Product Category": "string", "ogIngredientList": "string", "Leading product photo URL": "string", "Product description": "string", "SAFER BLACK BEAUTY BRAND PRODUCT?": "string"})
products2024 = pd.read_excel(productDataPath, "2024 products",
                             dtype={"productID": "string", "Brand": "string", "Product URL": "string", "Product name": "string", "Breadcrumbs": "string", "productCategory": "string", "Product Type": "string", "ogIngredientList": "string", "Leading product photo URL": "string", "Product description": "string", "SAFER BLACK BEAUTY BRAND PRODUCT IN 2022?": "string"})
ingredientLists = pd.read_excel(productDataPath, "Ingredient lists", dtype="string")
ingredientCombos = (pd.read_excel(productDataPath, "List - ingredient combos",
                                  usecols=[1, 2, 3, 4],
                                  dtype={"ingredientList": "string", "ingredientOrder": "int", "ingredient1": "string", "ingredient2": "string"})
                    .drop_duplicates()
                    )
ingredients = (pd.read_excel(productDataPath, "Ingredients", dtype="string")
               .merge(identifiedIngredients, "left", left_on="ingredient2", right_on="ingredientName")
               )

"""Let's create the following dataframes that will serve as the dataset to be
analyzed in the future

- 2022 products where each row is a product - ingredient combination
- 2024 products where each row is a product - ingredient combination
"""
data2022 = (products2022.merge(ingredientLists, "left", "ogIngredientList")
            .merge(ingredientCombos, "left", "ingredientList")
            .merge(ingredients, "left", ["ingredient1", "ingredient2"])
            .drop(columns=["ogIngredientList", "ingredient1", "URL", "Leading product photo URL"])
            .drop_duplicates()
            )
data2024 = (products2024.merge(ingredientLists, "left", "ogIngredientList")
            .merge(ingredientCombos, "left", "ingredientList")
            .merge(ingredients, "left", ["ingredient1", "ingredient2"])
            .drop(columns=["ogIngredientList", "ingredient1", "Product URL", "Leading product photo URL"])
            .drop_duplicates()
            )
# %%
"""Preparing to export the final dataset prior to analysis."""
note = ["This file contains the BCPP product dataset after I extensively",
        "cleaned the ingredient list data. The dataset is, I believe, ready",
        "for further analysis now, unless if there are fields in the dataset",
        "other than ingredients that I need to clean. To clean the ingredients,",
        "I first cleaned the ingredient lists in a semi-manual process using",
        "regular expressions to remove as much miscellaneous text as I could",
        "that would otherwise impede ingredient identification (e.g. 'active ingredient',",
        "'certified organic', etc.). I then separated the ingredient lists",
        "into individual ingredient names using regular expressions, then",
        "further cleaned these ingredient names. I then identified the resulting",
        "ingredient names using a combination of case-insensitive strict and fuzzy matching",
        "with CompTox and CosIng. I (1) performed a batch search on CompTox",
        "as a form of strict matching with CompTox, (2) performed strict matching",
        "between unidentified ingredients and a scraped copy of CosIng, (3)",
        "performed fuzzy string matching between ingredients that are still unidentified",
        "from step (2) with the CompTox batch search results from step (1), and",
        "then (4) performed fuzzy string matching between ingredients that",
        "could not be identified from step (3) with the scraped copy of CosIng.",
        "",
        "The tab 'Products 2022' contained the raw 2022 product dataset from",
        "the BCPP dataset. I added a 'productID' field to help count products",
        "in subsequent analyses. I also renamed the field that contained the",
        "original ingredient lists as 'ogIngredientList'; as a reminder, I then",
        "subjected this ingredient list to further cleaning before splitting it",
        "and cleaning the subsequent names. However, the values in this column",
        "are all original ingredient lists as they were obtained from BCPP.",
        "I also did the same thing with the tab 'Products 2024', which contains",
        "products from the year 2024 from BCPP.",
        "",
        "The tab 'Ingredient lists' contained the original ingredient lists",
        "from BCPP in the field 'ogIngredientList' as well as the ingredient",
        "lists after I cleaned them in the field 'ingredientList'. The",
        "ingredient lists in the column 'ingredientList' are split later into",
        "individual ingredient names as best as I could. I recorded this splitting",
        "in the tab 'Ingredient combos'. The column 'ingredient1' are the",
        "individual ingredients right after I splitted ingredient lists from",
        "'ingredientList', while the column 'ingredientOrder' records the order",
        "of the individual values from 'ingredient1'. I subjected the values",
        "from 'ingredient1' to further cleaning, resulting in cleaner names",
        "in the column 'ingredient2'. It is values in 'ingredient2' that I",
        "then subjected to the ingredient identification process.",
        "",
        "Ingredients, the process I did to identify them, and their identifiers",
        "are in the tab 'Ingredients'. To recap, this process was (1) strict",
        "matching with CompTox, (2) strict matching with CosIng, (3) fuzzy",
        "matching with CompTox, and then (4) fuzzy matching with CosIng.",
        "Ingredients that could not be identified from the previous step",
        "were subjected to the subsequent steps until I could either identify",
        "them or they reached the final step in the process. I noted in the",
        "column 'identificationMethod'. In this process, ingredients were",
        "'identified' if I could either matched them (either strict or fuzzy)",
        "to CompTox or INCI names on CosIng; the all-uppercase names that they were matched",
        "to on CompTox or CosIng are listed in the column 'identified'. If an",
        "ingredient name was identified using either fuzzy or strict matching",
        "with CompTox, then an all-uppercase ingredient name that have been",
        "identified from the CompTox batch search result in step (1) is listed",
        "in the column 'identified'. If an ingredient name was identified",
        "using CosIng instead (either strict or fuzzy), then the INCI name",
        "that they were matched to on CosIng is listed in the column 'identified'.",
        "For names that were identified using CosIng, not all of them have",
        "CAS RNs, as CosIng does not record CAS RNs for all of its chemical",
        "entries; of the ingredients that CosIng do have CAS RNs for, I then",
        "performed batch searches of their CAS RNs on CompTox. For ingredients",
        "that I identified using CosIng and that CosIng have CAS RNs, you will",
        "see, in this tab, that they have 2 distinct fields for CAS RNs; the",
        "field 'CosIngCASRN' lists the CAS RN that CosIng recorded for this",
        "ingredient while the field 'CompToxCASRN' lists the CAS RN that",
        "CompTox associates with this ingredient. CompTox records outdated",
        "CAS RNs that other databases such as CosIng might still use so that",
        "people who still use these outdated CAS RNs can still find the relevant",
        "information on the chemicals they are interested in on CompTox. Hence",
        "you might see ingredients identified using CosIng that might have",
        "2 different CAS RNs, with CosIng recording an older value while",
        "CompTox records a newer one. There are also plenty of other ingredients",
        "I identified using CosIng that have the same CAS RN between CosIng and",
        "CompTox. Also, note that ingredients identified using CosIng can have",
        "the 'CosIngCASRN' filled if CosIng records a CAS RN for them, but",
        "ingredients that were identified using CompTox will have this field",
        "blank.",
        "",
        "The field 'SMILES' is a string representing the chemical structure",
        "for some ingredients. I obtained this field from CompTox for",
        "substances that are on CompTox, either from substances I identified using CompTox",
        "or substances I identified using CosIng with CAS RNs that could also",
        "be found on CompTox. A SMILES is generally intended to represent what I call a",
        "'discrete structure', a known, specific structure ",
        "",
        "The sheets '2022 dataset' and '2024 dataset' are intended to be the",
        "final, cleaned datasets that are ready for analysis, and that anyone",
        "who wants to analyze them can simply read these datasets into an R",
        "or Python script. These sheets combine product identifying data",
        "(product name, description, ID, brand name), price, cleaned",
        "ingredient lists, and cleaned and identified ingredients. So far, I've",
        "only cleaned the ingredient data. Anyone who wants to analyze things",
        "other than ingredients might need to do some additional cleaning on",
        "other fields. Furthermore, there are still many, many ingredients",
        "that have not been identified. And of the ingredients that have been",
        "identified, some of their names are concatenated in ways that I could",
        "not separate, and so some names might actually be multiple ingredients",
        "concatenated together, and might include ingredients that I did not",
        "identify.",
        "",
        "Date of last cleaning: Thursday June 25, 2026"]
readMe = pd.DataFrame({"Note": note})
exportPath = outputFolder/"BCPP, cleaned and identified ingredients & product data.xlsx"
ingredientsExport = (ingredients.drop(columns=["ingredient1", "ingredientName"])
                     .drop_duplicates()
                     )
if os.path.exists(exportPath) is False:
    with pd.ExcelWriter(exportPath) as w:
        readMe.to_excel(w, "ReadMe", index=False)
        products2022.to_excel(w, "Products 2022", index=False)
        products2024.to_excel(w, "Products 2024", index=False)
        ingredientLists.to_excel(w, "Ingredient lists", index=False)
        ingredientCombos.to_excel(w, "Ingredient combos", index=False)
        ingredientsExport.to_excel(w, "Ingredients", index=False)
        data2022.to_excel(w, "2022 dataset", index=False)
        data2024.to_excel(w, "2024 dataset", index=False)
