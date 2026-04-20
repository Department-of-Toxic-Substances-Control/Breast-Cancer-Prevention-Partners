# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 10:56:22 2026

@author: BChung

I've searched the cleaned ingredient names on CompTox using the batch search
feature. Now, I'm going to see which ingredient names are still unidentified
after strict matching with CompTox and then match them against the EU CosIng
database using case-insensitive strict-matching again. I will then batch search
the CAS RNs that were identified after matching with EU CosIng.
"""
import os
from pathlib import Path
import pandas as pd

repository = Path(os.getcwd())
repositoryFolder = Path(os.path.dirname(repository))
dataFolder = repositoryFolder/"Data"
inputFolder = dataFolder/"Input"
outputFolder = dataFolder/"Output"

batchSearch1path = inputFolder/"Ingredients batch search 1.xlsx"
batchSearch1 = pd.read_excel(batchSearch1path, "Main Data")
batchSearch2path = inputFolder/"Ingredients batch search 2.xlsx"
batchSearch2 = pd.read_excel(batchSearch2path, "Main Data")
batchSearch3path = inputFolder/"Ingredients batch search 3.xlsx"
batchSearch3 = pd.read_excel(batchSearch3path, "Main Data")
batchSearchOG = pd.concat([batchSearch1, batchSearch2, batchSearch3],
                          ignore_index=True)

CosIngPath = inputFolder/"Cleaned CosIng database - scraped on January 21, 2026.xlsx"
CosIng = (pd.read_excel(CosIngPath, "Substances")
          .drop(columns=["Type", "Annex", "EC"])
          )
# %%
# Identifying ingredients by merging with CosIng

identifiedByBatchSearch = (batchSearchOG.query("DTXSID.notna()")
                           .rename(columns={"INPUT": "ingredientName"})
                           )
unidentifiedAfterBatchSearch = (batchSearchOG.query("DTXSID.isna()")
                                .filter(["INPUT"])
                                .rename(columns={"INPUT": "ingredientName"})
                                )
unidentifiedAfterBatchSearch["allCaps"] = unidentifiedAfterBatchSearch.ingredientName.str.upper()

unidentifiedAfterBatchSearch = unidentifiedAfterBatchSearch.merge(CosIng, "left", left_on="allCaps", right_on="INCI")

"""After this step, there are ingredients that have been identified using
CosIng (1) and have CAS RNs or (2) don't have CAS RNs, and there are also
ingredients that (3) still are unidentified. Let's split these into a few
dataframes. For the ingredients that have CAS RNs, I will batch search their
CAS RNs"""
identifiedStrictMatchCosIng = (unidentifiedAfterBatchSearch.query("INCI.notna()")
                               .drop(columns=["allCaps"])
                               )
identifiedCosIngCASRN = (identifiedStrictMatchCosIng.filter(["CASRN"])
                         .query("CASRN != '-'")
                         .drop_duplicates()
                         )
unidentifiedStrictMatchCosIng = (unidentifiedAfterBatchSearch.query("INCI.isna()")
                                 .filter(["ingredientName"])
                                 .drop_duplicates()
                                 )
# %%
"""Let's export an Excel file with the following tables and tabs

- ReadMe
- ingredients identified by case-insensitive strict matching with CompTox
- ingredients identified by case-insensitive strict matching with CosIng
- CAS RNs of the ingredients identified using CosIng
- ingredients that are still unidentified
"""

note = ["After searching ingredient names on CompTox using the batch search",
        "feature, I then performed case-insensitive strict matching with the",
        "CosIng database. This led to additional ingredients being identified",
        "but there are still many ingredient names that remain unidentified.",
        "The next steps are (1) searching the CAS RNs of some of the ingredients",
        "identified using CosIng on CompTox, then (2) do fuzzy string matching",
        "between unidentified ingredients and ingredients that have been",
        "identified using case-insensitive strict matching with ingredient names",
        "on CompTox and CosIng.",
        "",
        "The tab 'Identified CompTox' lists ingredients that were identified",
        "by searching their names using the batch search feature on CompTox.",
        "The tab 'Identified CosIng' lists ingredients that weren't",
        "identified using CompTox but were identified by case-insensitive",
        "strict matching with INCI names from CosIng. The tab 'CosIng CAS RN'",
        "lists the CAS RNs of ingredients identified from this step, if the",
        "ingredient has a CAS RN on CosIng. The tab 'Unidentified' lists",
        "ingredient names that still aren't identified after this step; these",
        "ingredients will undergo fuzzy string matching later."]
readMe = pd.DataFrame({"Note": note})
outputPath = outputFolder/"Identified & unidentified after batch search.xlsx"
if os.path.exists(outputPath) is False:
    with pd.ExcelWriter(outputPath) as w:
        readMe.to_excel(w, "ReadMe", index=False)
        identifiedByBatchSearch.to_excel(w, "Identified CompTox", index=False)
        identifiedStrictMatchCosIng.to_excel(w, "Identified CosIng", index=False)
        identifiedCosIngCASRN.to_excel(w, "CosIng CAS RN", index=False)
        unidentifiedStrictMatchCosIng.to_excel(w, "Unidentified", index=False)
