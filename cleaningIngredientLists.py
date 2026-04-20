# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 16:25:19 2026

@author: BChung

This script starts the process of cleaning ingredient lists in the BCPP data.
"""
import os
from pathlib import Path
import pandas as pd

repository = Path(os.getcwd())
repositoryFolder = Path(os.path.dirname(repository))
dataFolder = repositoryFolder/"Data"
inputFolder = dataFolder/"Input"
outputFolder = dataFolder/"Output"

datasetPath = inputFolder/"CSC Black Beauty Product Database and Screening Results.xlsx"
dataset = pd.ExcelFile(datasetPath)
# %%
"""I'm removing product entries that don't have any ingredient lists, such as
when this value is na or just consist of spaces."""

# Processing 2022 data
data2022og = (pd.read_excel(dataset, "2022_Black Beauty Products")
              .query("`Ingredient list`.notna()")
              .drop(columns=["Unnamed: 11", "Unnamed: 12"])
              .reset_index(drop=True)
              .reset_index()
              .rename(columns={"index": "productID", "Ingredient list": "ogIngredientList"})
              )
data2022og = data2022og.loc[~data2022og.ogIngredientList.str.isspace() & ~data2022og.ogIngredientList.str.contains("https://")]
data2022og.productID = data2022og.productID + 1
data2022og = data2022og.astype({"productID": "string"})
data2022og["productID"] = "2022-" + data2022og.productID

# Processing 2024 data
data2024columnsRename = {'Product Category\n\nselect from the list, or "other" products that don\'t fall into these product types.\n\nBabycare/Kidcare\nBodycare\nFragrance\nHaircare \nMakeup\nNail Products\nPersonal Hygiene\nSkin Care\nSunscreen\nOther': "productCategory", "Price \n$US": "priceUSD", "Price ": "price", "Ingredient list": "ogIngredientList"}
data2024og = (pd.read_excel(dataset, "2024_Black Beauty Products")
              .rename(columns=data2024columnsRename)
              .query("ogIngredientList.notna()")
              .reset_index(drop=True)
              .reset_index()
              .rename(columns={"index": "productID"})
              )
data2024og = data2024og.loc[~data2024og.ogIngredientList.str.isspace() & ~data2024og.ogIngredientList.str.contains("https://")]
data2024og.productID = data2024og.productID + 1
data2024og = data2024og.astype({"productID": "string"})
data2024og["productID"] = "2024-" + data2024og.productID
baskAndBoomFragrance = data2024og.loc[data2024og.productID == "2024-10333"]
data2024og = data2024og.loc[data2024og.productID != "2024-10333"]
# %%
"""Starting to process ingredient list data"""
ingredientList2022 = data2022og.ogIngredientList.drop_duplicates()
ingredientList2024 = data2024og.ogIngredientList.drop_duplicates()
ingredientLists = (pd.concat([ingredientList2022, ingredientList2024])
                   .drop_duplicates()
                   .to_frame()
                   .reset_index(drop=True)
                   )
ingredientLists["ingredientList"] = ingredientLists.ogIngredientList.str.strip()

"""There are some ingredient lists with 'FRAGRANCE INGREDIENTS' towards the
end, and this then lists fragrance ingredients. Let's identify ingredient lists
with this component."""
ingredientLists = ingredientLists.loc[~ingredientLists.ingredientList.str.contains("FRAGRANCE INGREDIENTS")]
fragranceIngredientsSection = ingredientLists.loc[ingredientLists.ingredientList.str.contains("FRAGRANCE INGREDIENTS")]

"""Stripping various bits and pieces from ingredient lists"""
naturallyDerivedStart = r"^\(97(\.\d)?% Naturally Derived.*\) Ingredients: "
derivedFrom = r"\s?\((\w+\s)?Derived from[^)]+\)"
startingIngredients = r"^(\w+ )?Ingredients? ?: ?"
ingredientsMiddle = r"(\w+ )?Ingredients?: ?"
colonText = r"\.[^:]+: "
otherBeginningText = r"^(Vegetable Oil Blend \(|\(All Organic\))"
andDelimiter = r", and | \(and\) |(?<!,) and |, & *"
mayContain = r"(May Contain|MAY CONTAIN)( \(+/-\))?:?"
ingredientLists["ingredientList"] = (ingredientLists.ingredientList.str.replace(naturallyDerivedStart, "")
                                     .str.replace(" ,", ",")
                                     .str.replace(", {2,5}", ", ")
                                     .str.replace(r"[*+~]+(?=[A-Z])", "")
                                     .str.replace(derivedFrom, "")
                                     .str.replace(r"(?<=\w)\*+, ", ", ")
                                     .str.replace(r"(?<=\w)\*+", "")
                                     .str.replace(startingIngredients, "")
                                     .str.replace(ingredientsMiddle, "")
                                     .str.replace("Ghanaian Coconut + Moroccan Almond Butter, ", "", regex=False)
                                     .str.replace(colonText, ", ")
                                     .str.replace(r"\.\s+", ", ")
                                     .str.replace("\n", ", ")
                                     .str.replace("Coffea sp, (Coffee Berry) Aqueous Extract", "Coffea sp. (Coffee Berry) Aqueous Extract", regex=False)
                                     .str.replace(otherBeginningText, "")
                                     .str.replace(andDelimiter, ", ")
                                     .str.replace(r"^% Food Grade H2O2", "H2O2")
                                     .str.replace(r"^\( Aqua/Water/Eau", "Aqua")
                                     .str.replace(", \(\+/-?\): ", ", ")
                                     .str.replace(mayContain, ", ")
                                     .str.replace(r"\(\+/-\):? *", "")
                                     .str.replace("(Aqua) Cocamidopropyl Hydroxysultaine", "Aqua, Cocamidopropyl Hydroxysultaine", regex=False)
                                     .str.replace("(CI 15850),(CI 15850)", "CI 15850", regex=False)
                                     .str.replace("(Pomegranate Seed Extract) Certified Organic Aloe Barbadensis Juice", "Pomegranate Seed Extract, Aloe Barbadensis Juice", regex=False)
                                     .str.replace(r"\(Organic\) (?=\w)", "")
                                     .str.replace("(Camellia sinensis),Green Tea extract", "Camellia sinensis extract", regex=False)
                                     .str.replace(r"\(Pro-Vitamin B5\) Panthenol,(?!\s)", "Provitamin B5, ")
                                     .str.replace(r"\(Pro-Vitamin B5\) Panthenol", "Provitamin B5")
                                     .str.replace("Vitamin blend- A, D, E, (M)* Macadamia", "Vitamin A, Vitamin D, Vitamin E, Macadamia", regex=False)
                                     .str.replace("Tocopherol (Vitamin E) Triticum Vulgare (Wheat) Germ Oil", "Tocopherol, Wheat Germ Oil", regex=False)
                                     .str.replace(r"Sclerocarya Birrea \(Marula Oil\) Adasaonia digitata \( *Organic Baobab Oil\) Mixed Tocopherol Non GMO \(Vitamin E\)", "Marula Oil, Adansonia Digitata Oil, Tocopherol")
                                     .str.replace("Non-GMO Tocopherol (Vitamin E) Pelargonium Graveolens (Geranium) Oil", "Tocopherol, Pelargonium Graveolens Oil", regex=False)
                                     .str.replace("Rosemary Extract (Rosmarimus Officinalis) Vitamin E (Tocopherol Acetate)", "Rosmarinus Officinalis oil, Tocopheryl Acetate", regex=False)
                                     .str.replace("Salix Nigra (Willow Bark Extract) Tocopherol (Vitamin E)", "Salix Nigra Bark Extract, Tocopherol", regex=False)
                                     .str.replace("Tocopherol Acitaetate (Vitamin E)", "Tocopheryl Acetate", regex=False)
                                     .str.replace(r"WATER,? ?\(AQUA((, |/)EAU)?\)", "WATER")
                                     .str.replace("Mica (CI 77019) Titanium Dioxide (CI 779891) Iron Oxide (CI 77491)", "Mica, Titanium Dioxide, CI 77491", regex=False)
                                     .str.replace("IRON OXIDES (CI 77491) MICA", "CI 77491, Mica", regex=False)
                                     .str.replace("CI 77491 & CI 77266", "CI 77491, CI 77266", regex=False)
                                     .str.replace(r"(IRON OXIDES?|Iron Oxides?) \(?CI 77491/(CI ?)77492/CI ?77499\)?", "CI 77491, CI 77492, CI 77499")
                                     .str.replace("+/- CI 77491/77492 (Iron Oxides)", "CI 77491, CI 77492", regex=False)
                                     .str.replace("CI 77489 CI 77491 CI 77492 CI 77499 (Iron Oxides)", "CI 77489, CI 77491, CI 77492, CI 77499", regex=False)
                                     .str.replace("]: Titanium Dioxide (CI 77891) Iron Oxides (CI 77491", "Titanium Dioxide, CI 77491", regex=False)
                                     .str.replace("+/- CI 77491/77492/77499 (Iron Oxides)", "CI 77491, CI 77492, CI 77499", regex=False)
                                     .str.replace("]: Iron Oxides (CI 77499) (CI 77491)", "CI 77499, CI 77491", regex=False)
                                     .str.replace("Titanium Dioxide (CI 77891) Iron Oxide (CI 77491/CI 774492/CI 77499) Ultramarine Blue (CI 77007) Chromium Oxide Green (CI 77288) Hydrated Chromium Oxide Green (CI 77289) Manganese violet (CI 77742) FD&C Yellow 5AL Lake (CI 19140)", "Titanium Dioxide, CI 77491, CI 77492, CI 77499, Ultramarine Blue, Chromium Oxide Green, C.I. 77829, Manganese violet, CI 19140", regex=False)
                                     .str.replace("CI 77491 - CI 77492 - 77499 (Iron Oxides)", "CI 77491, CI 77492, CI 77499", regex=False)
                                     .str.replace("CI 77499 / CI 77491", "CI 77499, CI 77491", regex=False)
                                     .str.replace("Bismuth, Oxychloride", "Bismuth Oxychloride")
                                     .str.replace(r"(Iron )?Oxides (CI 77499 ?/ CI 77491)", "CI 77499, CI 77491")
                                     .str.replace(") Titanium Dioxide (CI 77891) Iron Oxides (CI 77491", "Titanium Dioxide, CI 77491", regex=False)
                                     .str.replace("CI ?77489 CI ?77491 CI ?77492 CI ?77499 (Iron Oxides)", "CI 77489, CI 77491, CI 77492, CI 77499")
                                     .str.replace("Iron Oxides (CI 77491 CI 77492 CI 77499)", "CI 77491, CI 77492, CI 77499", regex=False)
                                     .str.replace("CI 77491 (IRON OXIDES) CI 45410 (RED 28 LAKE)", "CI 77491, CI 45410", regex=False)
                                     .str.replace("CI 77491/CI 77492/CI 77499 (Iron Oxides)", "CI 77491, CI 77492, CI 77499", regex=False)
                                     .str.replace("IRON OXIDES(CI 77491) FELA: DIMETHICONE", "CI 77491, DIMETHICONE", regex=False)
                                     .str.replace("CI 77491 KUSH: CALCIUM SODIUM BOROSILICATE", "CI 77491, CALCIUM SODIUM BOROSILICATE", regex=False)
                                     .str.replace("Iron Oxide (CI 77491) Titanium Dioxide (CI 77891)", "CI 77491, Titanium Dioxide", regex=False)
                                     .str.replace("IRON OXIDES (CI 77491/77492/CI77499)", "CI 77491, CI 77492, CI 77499", regex=False)
                                     .str.replace("Iron Oxide (CI 77491/CI 77492/CI 77299)", "CI 77491, CI 77492, CI 77499", regex=False)
                                     .str.replace("Oxides (CI 77499/ CI 77491)", "CI 77499, CI 77491", regex=False)
                                     .str.replace("CI77489 CI 77491 CI 77492 CI 77499 (Iron Oxides)", "CI 77489, CI 77491, CI 77492, CI 77499", regex=False)
                                     .str.replace("CI 77489 CI 77491 CI 77492 CI77499 (Iron Oxides)", "CI 77489, CI 77491, CI 77492, CI 77499", regex=False)
                                     .str.replace("The Standard-Mica (CI 77019) Titanium Dioxide (CI 77891) Iron Oxide (CI 77492)", "Mica, Titanium Dioxide, CI 77492", regex=False)
                                     .str.replace(r"Ethylhexylglycerin [^\w]+May contain\): Titanium Dioxide \(C[Il] 77891\)", "Ethylhexylglycerin, Titanium Dioxide")
                                     .str.replace("Propylene Carbonate Titanium Dioxide (CI 77891)", "Propylene Carbonate, Titanium Dioxide", regex=False)
                                     .str.replace("Silica Dimethyl Silylate Titanium Dioxide (CI 77891)", "Silica Dimethyl Silylate, Titanium Dioxide", regex=False)
                                     .str.replace("Alpha-Isomethyl Ionone Titanium Dioxide (CI 77891)", "Alpha-Isomethyl Ionone, Titanium Dioxide", regex=False)
                                     .str.replace(r"TITANIUM DIOXIDE\(CI 77891\)[\w\s]+: (?=TALC|OCTYLDODECYL STEAROYL STEARATE|SYNTHETIC FLUORPHLOGOPITE)", "TITANIUM DIOXIDE, ")
                                     .str.replace("Triethoxycaprylylsilane May contain +/- Titanium Dioxide", "Triethoxycaprylylsilane, Titanium Dioxide", regex=False)
                                     .str.replace("[+/- Ci 77891 (Titanium dioxide) Ci 77491 (Iron oxides)", "Titanium dioxide, CI 77491", regex=False)
                                     .str.replace("MICA (CI 77019) AND TITANIUM DIOXIDE (CI 77891)", "Mica, Titanium Dioxide", regex=False)
                                     .str.replace("+/- Mica (CI 77891)", "Mica", regex=False)
                                     .str.replace("Titanium Dioxide 2.0% (sunscreens) Ethylhexyl Palmitate", "Titanium Dioxide 2.0%, Ethylhexyl Palmitate", regex=False)
                                     .str.replace("Titanium Dioxide 9CI 77891)", "Titanium Dioxide", regex=False)
                                     .str.replace("Titanium dioxide (CI 77492)", "Titanium dioxide", regex=False)
                                     .str.replace("Octinoxate 2.13% Titanium Dioxide 2.66%", "Octinoxate 2.13%, Titanium Dioxide 2.66%", regex=False)
                                     .str.replace("Titanium Dixoide (Ci 77891)", "Titanium Dioxide", regex=False)
                                     .str.replace("Iron Oxides (CI 77891", "Iron Oxides (CI 77491", regex=False)
                                     .str.replace("CI 77891 (TITANIUM, DIOXIDE)", "Titanium Dioxide", regex=False)
                                     .str.replace("Titanium Dioxide/ Oxides 77499", "Titanium Dioxide, CI 77499", regex=False)
                                     .str.replace(r"(Matte|Metallic) Shades Ingredients \([^)]+\): ", "")
                                     .str.replace("May also contain Emulsifying Wax or Cetyl Alcohol", "Emulsifying Wax, Cetyl Alcohol", regex=False)
                                     .str.replace("Urtica Dioica (Nettle) Extract Honey (Mel) Wildflower & Clover", "Urtica Dioica Extract, Honey", regex=False)
                                     .str.replace("(Prunus Serotina) Propanediol", "Propanediol", regex=False)
                                     .str.replace("(WHITE AND TURBINADO SUGAR BLEND) SUCROSE", "SUCROSE", regex=False)
                                     .str.replace(r"[Bb]enzyl [Aa]lcohol( \(Plant Derived.+Soluble\))? [Dd]ehydroacetic [Aa]cid( \(non-drying alcohol.+preservation system\))?", "Benzyl Alcohol, Dehydroacetic Acid")
                                     .str.replace("Benzyl alcohol Ethylhexylglyc-, erin", "Benzyl alcohol, Ethylhexylglycerin", regex=False)
                                     .str.replace("PPG-12 SMDI Copolymer Benzyl Alcohol", "PPG-12/SMDI Copolymer, Benzyl Alcohol", regex=False)
                                     .str.replace("Citrus Grandis (Grapefruit) Peel Oil / Lavandula Angustifolia (Lavender) Oil / Eugenia Caryophyllata (Clovebud) Oil", "Grapefruit Oil, Lavandula Angustifolia Oil, Clove bud Oil", regex=False)
                                     .str.replace("Eucalyptus Globulus (Eucalyptus) Essential Oil + Lavandula Angustifolia (Lavender) Essential Oil", "Eucalyptus Globulus Oil, Lavandula Angustifolia Oil", regex=False)
                                     .str.replace("Eucalyptus Globulus Leaf Oil & Lavandula Angustifolia (Lavender) Oil", "Eucalyptus Globulus Leaf Oil, Lavandula Angustifolia Oil", regex=False)
                                     .str.replace(r"Grapefruit( & |/)Lavender [Ee]ssential [Oo]il(s| \([12]%\))", "Grapefruit Oil, Lavender Oil")
                                     .str.replace("Corn flour (plus essential oils for the lemongrass & lavender variants)", "Corn flour", regex=False)
                                     .str.replace(r"(Chebe powder \()?Lavender Croton", "Croton Gratissimus Seed Extract")
                                     .str.replace(r"LAVANDULA ANGUSTIFOLIA \(LAVENDER OIL\) (?=SORBIC ACID|TOCOPHEROL)", "LAVANDULA ANGUSTIFOLIA OIL, ") 
                                     .str.replace("Colloidal Oatmeal & Lavender Essential Oil", "Colloidal Oatmeal, Lavender Oil", regex=False)
                                     .str.replace("Lavandula Angustifolia (Lavender) Oil Althaea Officinalis (Marshmallow) Leaf/Root Extract", "Lavandula Angustifolia Oil, Althaea Officinalis Leaf/Root Extract", regex=False)
                                     .str.replace("Lavandula Angustifolia (Lavender) Oil or Citrus Grandis (Grapefruit) Peel Oil", "Lavandula Angustifolia Oil, Citrus Grandis Peel Oil", regex=False)
                                     .str.replace("Lavandula Officinalis Flower Oil (Lavender Essential Oil) Sodium Hydroxide", "Lavandula Officinalis Flower Oil, Sodium Hydroxide", regex=False)
                                     .str.replace("Lavandula angustifolia (Lavender) Flower ExtractAloe Barbadensis Leaf Juice", "Lavandula angustifolia Flower Extract, Aloe Barbadensis Leaf Juice", regex=False)
                                     .str.replace("Lavandula hy -brida (Lavender) Oil", "Lavandula hybrida Oil", regex=False)
                                     .str.replace("Jasmine + Lavender Oil", "Jasmine Oil, Lavender Oil", regex=False)
                                     .str.replace("Lavender & Bergamot essential oil", "Lavender Oil, Bergamot Oil", regex=False)
                                     .str.replace("Lavender & Chamomile Essential Oils", "Lavender Oil, Chamomile Oil", regex=False)
                                     .str.replace("Lavender & Lemon Oils Water", "Lavender Oil, Lemon Oil", regex=False)
                                     .str.replace(r"Lavender & Vanilla ?|vanilla & lavender essential oils", "Lavender Oil, Vanilla extract")
                                     .str.replace("CI 77499 Essential Oils: Lavender", "CI 77499, Lavender Oil", regex=False)
                                     .str.replace("CO Lavandula Angustifolia (Lavender) Flower/Leaf/Stem Extract", "Lavandula Angustifolia Flower/Leaf/Stem Extract", regex=False)
                                     .str.replace(r"LAVANDULA ANGUSTIFOLIA \(LAVENDER\)( ESSENTIAL)? ", "LAVANDULA ANGUSTIFOLIA ")
                                     .str.replace("Lavender Oil Rosmarinus Officinalis (Organic Rosemary) Leaf Extract", "Lavender Oil, Rosmarinus Officinalis Leaf Extract", regex=False)
                                     .str.replace("Lavender-Vanilla Scented", "Lavender Oil, Vanilla Extract", regex=False)
                                     .str.replace(r"Lavender (Essential Oil )?[+&] Vanilla Essential Oil(s| Blend)", "Lavender Oil, Vanilla Extract")
                                     .str.replace("Lemongrass & Lavender Flowers in Coconut Oil", "Oil of lemongrass, Lavender Oil", regex=False)
                                     .str.replace("Lavender Buds & Cornflower Petals", "Lavender flower oil, Centaurea Cyanus Flower", regex=False)
                                     .str.replace("Peppermint & Lavender Essential Oils", "Peppermint Oil, Lavender Oil", regex=False)
                                     .str.replace("Prunus Armeniaca (Apricot) Kernel Oil Infused with (Lavandula angustifolia (Lavender) Flowers", "Prunus Ameniaca Kernel Oil, Lavandula Angustifolia Flower Extract", regex=False)
                                     .str.replace("Pure Steam Distilled Essential Oils of Lemongrass & Lavender", "Oil of lemongrass, Lavender Oil", regex=False)
                                     .str.replace(r"Rosemary (Essential Oil )?& Lavender Essential Oil(s? & Fragrance)?", "Rosmarinus officinalis oil, Lavender Oil")
                                     .str.replace("Vegetable Glycerine & Lavender Essential Oil", "Glycerin, Lavender Oil", regex=False)
                                     .str.replace("b-caryophyllene - naturally occurring constituents of Lavender essential oil", "b-caryophyllene", regex=False)
                                     .str.replace("Nettle & Lavender Extracts", "Urtica Dioica Extract, Lavender Oil", regex=False)
                                     .str.replace("Organic Essential Oils of Lavender & Roman Chamomile", "Lavender Oil, Anthemis Nobilis Flower Oil", regex=False)
                                     .str.replace("Rose Clay & Lavender Essential Oil", "Kaolin, Lavender Oil", regex=False)
                                     .str.replace("lavandula angustifolia (lavender) & perilla frutescens (shiso leaf)", "lavandula angustifolia extract, perilla frutescens leaf extract", regex=False)
                                     .str.replace("lavandula angustifolia (lavender) flower water green tea extract", "lavandula angustifolia flower water, green tea extract", regex=False)
                                     .str.replace(r"[Aa]ugustif[ou]li(a|um)|[Aa]ngus(to?|z)folia|[Aa]gustifolia|[Aa]ngustifulia|[Aa]ngusttifolia", "Angustifolia")
                                     .str.replace(r"[Ll]av(a|en)dula|LAV(A|EN)DULA|[Ll]anvandula|LANVANDULA", "Lavandula")
                                     .str.replace("Lavender Essential Oilccharomyces/Grape Ferment Extract & Lactobacillus/Acerola Cherry Ferment", "Lavender Oil, Saccharomyces/Grape Ferment Extract, Lactobacillus/Acerola Cherry Ferment", regex=False)
                                     .str.replace("Lavender Honeysuckle Essential Oil Blend", "Lavender Oil", regex=False)
                                     .str.replace("Lavendula X Intermedia (Lavender) Flower", "Lavandula Hybrida Extract", regex=False)
                                     .str.replace("ground lavender + chamomile flowers", "Lavender Oil, Chamomile Oil", regex=False)
                                     .str.replace("lavender & calendula", "Lavender Oil, Calendula Extract", regex=False)
                                     .str.replace("lavender + clary sage essential oils", "Lavender Oil, Salvia Sclarea Oil", regex=False)
                                     .str.replace("lavender + palmarosa", "Lavender Oil, Palmarosa Oil", regex=False)
                                     .str.replace("lavender + peppermint essential oils", "Lavender Oil, Peppermint Oil", regex=False)
                                     .str.replace("lavender Mind At Ease: Rosemary", "Lavender Oil, Rosmarinus Officinalis Leaf Extract", regex=False)
                                     .str.replace("lavender Zen Retreat: Patchouli", "Lavender Oil, Patchouli extract", regex=False)
                                     .str.replace("orange + lavender essential oils", "Citrus Sinensis Peel Oil Expressed, Lavender Oil", regex=False)
                                     .str.replace("; ", ", ", regex=False)
                                     .str.replace("tea tree & lavender essential oils", "tea tree oil, Lavender Oil", regex=False)
                                     .str.replace("tocopherol (vitamin E) Lavandula (lavender)", "Tocopherol, Lavander Oil", regex=False)
                                     .str.replace("Sea Lavender (Limonium Vulgare)", "Limonium Vulgare flower/leaf/stem extract", regex=False)
                                     .str.replace(r"[Ll]avande(?!r)|[Ll]avendar", "Lavender")
                                     .str.replace("0il", "Oil", regex=False)
                                     .str.replace("Lavandula Angustifolia Oil & Chamomilla Recutita Flower Oil", "Lavandula Angustifolia Oil, Chamomilla Recutita Flower Oil", regex=False)
                                     .str.replace("(Tapioca Starch) & Lactobacillus Ferment Lysate", "Tapioca Starch, Lactobacillus Ferment Lysate", regex=False)
                                     .str.replace("(extraits de fleurs)", "", regex=False)
                                     .str.replace(" Ricinus communism (castor oil, ) ", ", castor oil, ", regex=False)
                                     .str.replace("*/Organic Rosewood Essential Oil (Aniba Roseaodora)", "Aniba Rosaeodora Wood Oil", regex=False)
                                     .str.replace(r"AND PHENOXYETHANOL AND CAPRYLYL GLYCOL( \(OPTIPHEN\))?", "PHENOXYETHANOL, CAPRYLYL GLYCOL")
                                     .str.replace("Aqueous (Purified Water) Extracts: Aloe Barbadensis (Aloe Vera) Oil", "Water, Aloe Barbadensis", regex=False)
                                     .str.replace("Aqueous (Purified Water) organic infusion of Agathosma Betulina (Buchu) Leaf", "Water, Agathosma Betulina Leaf Extract", regex=False)
                                     .str.replace("Lavender Flowers in Purified Water with Vitamin C (ascorbic acid)- contains less than 1% preservative blend (propylene glycol", "Water, Lavender Oil, Ascorbic Acid", regex=False)
                                     .str.replace("Marshmallow Root Cleansing Conditioner: Purified Water (infused with natural herbs: Marshmallow Root", "Water, Althaea Officinalis Flower Extract", regex=False)
                                     .str.replace("Purified Water & Organic Rose Petals", "Water, Rose Extract", regex=False)
                                     .str.replace("Purified Water (and)Pyrus Malus (Apple) Fruit Extract", "Water, Apple Extract", regex=False)
                                     .str.replace("Purified Water infused with natural Aloe Vera", "Water, Aloe Vera", regex=False)
                                     .str.replace("Mentha Piperita (Peppermint) Oil Benzoic Acid", "Mentha Piperita Oil, Benzoic Acid", regex=False)
                                     .str.replace("Frigg’s Essential Oil Blend: Mentha Piperita (Peppermint) Oil (Pimenta Racemosa (Bay) Essential Oil", "Mentha Piperita Oil, Pimenta Racemosa Leaf/Fruit Oil", regex=False)
                                     .str.replace(r"behentrimonium methosulfate \(BTMS\)(?= ?c)", "behentrimonium methosulfate, ")
                                     .str.replace(r"[Aa]vacado", "Avocado")
                                     .str.replace("botanical hyaluronic acid)", "hyaluronic acid", regex=False)
                                     .str.replace("Kaolin Clay & Colloidal Oatmeal", "Kaolin, Colloidal Oatmeal", regex=False)
                                     .str.replace("Essential Oil Vitamin E oil", "Vitamin E", regex=False)
                                     .str.replace("Fragrance: Essential oils of Lemon", "Lemon Oil", regex=False)
                                     .str.replace("Lemongrass Essential Oil & Vitamin E", "Lemongrass Oil, Vitamin E", regex=False)
                                     .str.replace("/Parfum Citric Acid", "Citric Acid", regex=False)
                                     .str.replace("/ Peut Contenir Iron Oxides (Ci 77491)", "CI 77491", regex=False)
                                     .str.replace("Non-GMO & Sustainable PalmSorbitol", "Palm Oil, Sorbitol", regex=False)
                                     .str.replace("polyquaternium,-11", "polyquaternium-11, ", regex=False)
                                     .str.replace("Fragrance Oil Blend & Optiphen", "Caprylyl Glycol, Phenoxyethanol", regex=False)
                                     .str.replace("Fragrance (Parfum) Caramel", "Fragrance, Caramel", regex=False)
                                     .str.replace("Fragrance (Parfum) Red 33 (CI 17200)", "Fragrance, CI 17200", regex=False)
                                     .str.replace("Fragrance Oil & Vitamin E", "Fragrance, Vitamin E", regex=False)
                                     .str.replace("fragrance (phthalate free) beta carotene", "fragrance, beta carotene", regex=False)
                                     .str.replace("fragrance of Passion Flower", "Passiflora Incarnata Flower Extract", regex=False)
                                     .str.replace("fragrance: hints of lemon", "Lemon Oil", regex=False)
                                     .str.replace("Laureth-23 Dimethiconol", "Laureth-23, Dimethiconol", regex=False)
                                     .str.replace("Laureth-4 Polyquaternium-10", "Laureth-4, Polyquaternium-10", regex=False)
                                     .str.replace("CetTimonium Chloride", "Cetrimonium Chloride", regex=False)
                                     .str.replace(r"Cetearyl Alcohol( & | | \(and\))Ceteareth-20( \(Emulsifying Wax\))?", "Cetearyl Alcohol, Ceteareth-20")
                                     .str.replace(r"Cetearyl Alcohol (& )?Cetearyl Glucoside( \(Sugar-based Emulsifier\))?", "Cetearyl Alcohol, Cetearyl Glucoside")
                                     .str.replace(r"(Cetearyl Alcohol|CETEARYL ALCOHOL) (& |\(AND\) )?(Polysorbate|POLYSORBATE) 60", "Cetearyl Alcohol, Polysorbate 60")
                                     .str.replace("Cetearyl Alcohol/ Cetrimonium Bromide", "Cetearyl Alcohol, Cetrimonium Bromide", regex=False)
                                     .str.replace("Cetearyl Chloride Cetyl Alcohol", "Cetearyl Chloride, Cetyl Alcohol", regex=False)
                                     .str.replace("Cetearyl Alcohol (Cetyl Alcohol)", "Cetyl Alcohol", regex=False)
                                     .str.replace("Squalane Magnesium, Sterate, Tocopherly Acetate", "Squalane, Magnesium Stearate, Tocopheryl Acetate", regex=False)
                                     .str.replace(r"Cetyl Alcohol \([\s\w]+\) (?=\w)", "Cetyl Alcohol, ")
                                     .str.replace("Cetyl Alcohol;Behenic Acid", "Cetyl Alcohol, Behenic Acid", regex=False)
                                     .str.replace("This blend contains the following in their natural compositions Linalool", "Linalool", regex=False)
                                     .str.replace("natural ingredients including olive oil", "olive oil", regex=False)
                                     .str.replace("(Glycerin) Glycerin", "Glycerin", regex=False)
                                     .str.replace(r"1.{1,4}2- ?([Hh]exane?d(io|oi)l|HEXANE?D(IO|OI)L)", "1,2-Hexanediol")
                                     .str.replace(r"1, 2-([Hh]exanediol|HEXANEDIOL)", "1,2-Hexanediol")
                                     .str.replace(r"([Zz]inc [Oo]xide|ZINC OXIDE)[^,]+?([Aa]llantoin|ALLANTOIN)", "Zinc Oxide, Allantoin")
                                     .str.replace("Zinc Oxide (17.5%) Dimethicone", "Zinc Oxide, Dimethicone", regex=False)
                                     .str.replace(r"1-([Hh]exadecanol [Aa]lcohol|HEXADECANOL ALCOHOL)", "1-Hexadecanol")
                                     )

"""Now I'm going to do some final editing of ingredient lists that's kind of
like copyediting to fix any minor artifacts that my cleaning had created, such
as multiple spaces or multiple pairs of commas and spaces"""
ingredientLists["ingredientList"] = (ingredientLists.ingredientList.str.replace(r"(, ){2,10}", ", ")
                                     .str.replace(r" {2,10},", ",")
                                     .str.replace(r", {2,10}", ", ")
                                     .str.replace(r",{2,10}", ",")
                                     .str.replace(r"(?<! ),(?! )", ", ")
                                     .str.replace(" {2,10}", " ")
                                     )
# %%
"""Splitting ingredient lists and cleaning the individual ingredient names"""
splitIngredients = (ingredientLists.ingredientList.str.split(", ", expand=True)
                    .join(ingredientLists)
                    .melt(["ogIngredientList", "ingredientList"], var_name="ingredientOrder", value_name="ingredient1")
                    .query("ingredient1.notna() & (ingredient1 != '')")
                    )

ingredientsDF = (splitIngredients.filter(["ingredient1"])
                 .drop_duplicates()
                 )

ingredientsDF["ingredient2"] = ingredientsDF.ingredient1.str.strip()
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^\([*+]\)")]
ingredientsDF.ingredient2 = ingredientsDF.ingredient2.str.rstrip(".")
substringRemove = r"Contains Carmine|Spiced Orchard Scent|#27|is when an oil|plant derived squalane|(^\(\* Certified Organic)|(^\(\*organic\))|COOKIE SHOTS|ORANGUTAN|Fallen Rose Scent|& e|AND PHTHALATE[- ]FREE"
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(substringRemove)]
ingredientsDF["ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\(\+/-\) ", "")
ingredientsDF = ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"\w")]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\(") & ingredientsDF.ingredient2.str.contains(r"\)$"), "ingredient2"] = ingredientsDF.ingredient2.str.strip("()")
ingredientsDF.loc[ingredientsDF.ingredient1 == "(Vitamin E) Oil", "ingredient2"] = "Vitamin E"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\(") & ingredientsDF.ingredient2.str.contains(r"\)( \w+)? ([Oo]il|OIL)"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"[()]", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\(([Vv]egetable|VEGETABLE)\) \w+$"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\(([Vv]egetable|VEGETABLE)\) ", "")
ingredientsDF.loc[ingredientsDF.ingredient2 == "( +/-): Mica", "ingredient2"] = "Mica"
ingredientsDF.loc[ingredientsDF.ingredient2 == "+/- ): Titanium Dioxide (CI 77891", "ingredient2"] = "Titanium Dioxide"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\): *"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\): *", "")
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^\* =")]
ingredientsDF.loc[ingredientsDF.ingredient1.str.contains(r"\(\w+\) Clay"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r" Clay$", "")
ingredientsDF.loc[ingredientsDF.ingredient1.str.contains(r"\(\w+\) Clay"), "ingredient2"] = ingredientsDF.ingredient2.str.strip("()")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\(Butyrospermum Parkii \(Shea Butter"), "ingredient2"] = "Shea Butter"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"HYDRA-pHUSION BLEND"), "ingredient2"] = "Deionized Water"
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^\* ([Cc]ertified|Derived|FOR EXTERNAL|Fair Trade|Naturally occurring|No Sodium|[Oo]rganic|Phthalate|ingredients? (issued|marked)|A natural preservative|Adults & Kids|Contains over|Made with|denotes a certified)")]
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^\*{2}[^\s]")]
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^\*{2} (?!Yucca)")]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\*{2} Yucca"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\*{2} ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"(?<!Alpha )Tocopherol(?! Acetate)") & ingredientsDF.ingredient2.str.contains("Vitamin E"), "ingredient2"] = "Tocopherol"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"(?<!D )Alpha Tocopherol") & ingredientsDF.ingredient2.str.contains("Vitamin E"), "ingredient2"] = "Alpha Tocopherol"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"D Alpha Tocopherol") & ingredientsDF.ingredient2.str.contains("Vitamin E"), "ingredient2"] = "D Alpha Tocopherol"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"Tocopherol Acetate"), "ingredient2"] = ingredientsDF.ingredient2.str.replace("Tocopherol Acetate", "Tocopheryl Acetate")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains("Tocopheryl Acetate") & ingredientsDF.ingredient2.str.contains("Vitamin E"), "ingredient2"] = "Tocopheryl Acetate"
ingredientsDF.loc[ingredientsDF.ingredient2 == "( Vitamin E", "ingredient2"] = "Vitamin E"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^.{3}(GARDENIA|PANAX)"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^.{3}(?=GARDENIA|PANAX)", "")
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^\*(derived from|essential oil|ingredients issued|natural|occurs naturally|organic(?!.)|organic (?!o)|organical|palm free|=?certified)")]
ingredientsDF.loc[ingredientsDF.ingredient2 == "(Horsetail ) Equisetum Arvense", "ingredient2"] = "Equisetum Arvense Extract"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\(") & ~ingredientsDF.ingredient2.str.contains(r"\)"), "ingredient2"] = ingredientsDF.ingredient2.str.lstrip("(")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains("CI 77491"), "ingredient2"] = "CI 77491"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Tt]itanium [Dd](ioxide|ixoide)|TITANIUM (DIOXIDE|DIXOIDE)") & ingredientsDF.ingredient2.str.contains("77891|[Cc][Iil]"), "ingredient2"] = "Titanium Dioxide"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^(Cl 77891|C[Iil]77891)$"), "ingredient2"] = "CI 77891"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^(May contain ){1,2}[^\w]*\w"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^(May contain ){1,2}(\(\+.-\))? ?", "")
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains("^May Also Contain.*:(?! \w)")]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains("^May Also Contain: "), "ingredient2"] = ingredientsDF.ingredient2.str.replace("May Also Contain: ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains("^May \+/-"), "ingredient2"] = ingredientsDF.ingredient2.str.replace("May +/- ", "", regex=False)
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^May (also )?contain:?$")]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^May contain: "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^May contain: ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"Cl \d{5}"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"Cl(?= \d{5})", "CI")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^Matte(s:)? (Mica|MICA|Synthetic)"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"Matte(s:)? ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Hh]oney|HONEY") & ingredientsDF.ingredient2.str.contains(r"([Mm]el|MEL)(?!\w)"), "ingredient2"] = "Mel"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\(\w+( \w+)?\) \w"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"[()]", "")
ingredientsDF.loc[ingredientsDF.ingredient2 == "(Cosmetic Grade Mica)- shimmer type only", "ingredient2"] = "Mica"
ingredientsDF.loc[ingredientsDF.ingredient2 == "(Theobroma Cacao Shell Powder)**", "ingredient2"] = "Theobroma Cacao Shell Powder"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\({1,2} \w"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\({1,2} ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r".([Bb]enzyl [Aa]lcohol|BENZYL ALCOHOL)|([Bb]enzyl [Aa]lcohol|BENZYL ALCOHOL).|.([Bb]enzyl [Aa]lcohol|BENZYL ALCOHOL)."), "ingredient2"] = "Benzyl Alcohol"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Aa]ngustifolia|ANGUSTIFOLIA|[Oo]fficinalis|OFFICINALIS") & ingredientsDF.ingredient2.str.contains(r"[Ll]avender|LAVENDER") & ingredientsDF.ingredient2.str.contains(r"[Oo]il|OIL|\b(EO|eo)\b"), "ingredient2"] = "Lavandula Angustifolia Oil"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Ll]avender|LAVENDER") & ingredientsDF.ingredient2.str.contains(r"[Oo]il|OIL|\b(EO|eo)\b") & ~ingredientsDF.ingredient2.str.contains(r"[Aa]ngustifolia|ANGUSTIFOLIA|[Ff]rench|FRENCH|[Ss]pica|SPICA|[Oo]fficinalis|OFFICINALIS|[Hh]ybrida|HYBRIDA|[Ll]atifolia|LATIFOLIA|[Ss]pike|SPIKE"), "ingredient2"] = "Lavender oil"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^LAVANDULA ANGUSTIFOLIA \(LAVENDER\)"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r" \(LAVENDER\)( ESSENTIAL)?", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Aa]ngustifolia|ANGUSTIFOLIA") & ingredientsDF.ingredient2.str.contains(r"[Ww]ater|WATER") & ingredientsDF.ingredient2.str.contains(r"[Hh]ydrosol|HYDROSOL"), "ingredient2"] = "Lavandula Angustifolia Water"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Hh]ydrosol|HYDROSOL") & ~ingredientsDF.ingredient2.str.contains(r"[Ww]ater|WATER"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"[Hh]ydrosol|HYDROSOL", "Water")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"([Ll]avandula|LAVANDULA) \w+ \(([Ll]avender|LAVENDER)\)"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r" \(([Ll]avender|LAVENDER)\) ?", " ")
ingredientsDF.ingredient2 = (ingredientsDF.ingredient2.str.strip()
                             .str.replace(r"^(\){1,2}|\*|\+/-) ", "")
                             .str.replace(r"^(\*|\+/-)", "")
                             )
ingredientsDF = ingredientsDF.loc[~(ingredientsDF.ingredient2.str.isnumeric() & ~ingredientsDF.ingredient2.str.contains(r"\d{5}"))]
ingredientsDF = ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[a-zA-Z]") | ingredientsDF.ingredient2.str.contains(r"\d{5}")]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\d "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\d ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^100% "), "ingredient2"] = ingredientsDF.ingredient2.str.replace("^100% ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^([Pp]ure|PURE|[Oo]rganic|ORGANIC|[Nn]atural|NATURAL|AND) "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"([Pp]ure|PURE|[Oo]rganic|ORGANIC|[Nn]atural|NATURAL|AND) ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Aa][gq]ua|A[GQ]UA") & (ingredientsDF.ingredient2.str.contains(r"[Ww]ater|WATER|\b([Ee]au|EAU)\b")), "ingredient2"] = "Aqua"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Ww]ater|WATER") & ingredientsDF.ingredient2.str.contains(r"\b([Ee]au|EAU)\b"), "ingredient2"] = "Water"
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^(AROMA|Aroma)\b")]
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^[Ee]ssential [Oo]ils?$")]
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^[Ee]ssential [Oo]ils? [Bb]lend[sy]?$")]
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^[Ee]ssential [Oo]ils[\s\w]+[Ff]ragrance( [Oo]ils?)?$")]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^& Fair Trade Certified™ "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^& Fair Trade Certified™ ", "")
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^\(fragrance\) [Oo]il$")]
ingredientsDF = ingredientsDF.loc[ingredientsDF.ingredient2.str.len() > 1]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\+"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\+ ?", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^: "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^: ", "")
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^A (?!blend of|special blend of Colloidal)")]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^ACTIVE INGREDIENTS?: "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^ACTIVE INGREDIENTS?: ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Pp]urified [Ww]ater|PURIFIED WATER"), "ingredient2"] = "Water"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^([Mm]entha [Pp]iperit?a|MENTHA PIPERIT?A) \(([Pp]eppermint|PEPPERMINT)\)") & ingredientsDF.ingredient2.str.contains(r"[Oo]il|OIL"), "ingredient2"] = "Mentha Piperita Oil"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"PEG|peg"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"(?<=PEG|peg) ?- ?(?=[1-9])", "-")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"\)$") & ~ingredientsDF.ingredient2.str.contains(r"\("), "ingredient2"] = ingredientsDF.ingredient2.str.rstrip(")")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^Kaolin(?!ite)"), "ingredient2"] = "Kaolin"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^Kaolinite"), "ingredient2"] = "Kaolinite"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^L ?- ?[Aa]scorbic [Aa]cid") & ingredientsDF.ingredient2.str.contains("Vitamin C"), "ingredient2"] = "L-Ascorbic Acid"


startingSubstringRemove = r"^(an e|antioxidants|botanical|Breathe Essential|E[OS]|No essential oil)"
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(startingSubstringRemove)]
ingredientsDF = ingredientsDF.loc[~(ingredientsDF.ingredient2.str.contains(r"^Essential Oil") & ingredientsDF.ingredient2.str.contains(r"[Ff]ragrance"))]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^Essential [Oo]ils? (([Oo]f|[Bb]lend)( |: ))?"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^Essential [Oo]ils? (([Oo]f|[Bb]lend)( |: ))?", "") + " Oil"
ingredientsDF = ingredientsDF.loc[~(ingredientsDF.ingredient2.str.contains(r"[Ff]ragran(ce|t)") & ingredientsDF.ingredient2.str.contains("[Ee]ssential [Oo]il"))]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Ee]ssential [Oo]ils?|ESSENTIAL OILS?|\b(EO|eo)\b"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"[Ee]ssential [Oo]ils?|ESSENTIAL OILS?|\b(EO|eo)\b", "Oil")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"( [Oo]il)+"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"( [Oo]il)+", " Oil")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\(") & ~ingredientsDF.ingredient2.str.contains("\)"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\(", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"\([Ff]ragrance\)"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"\([Ff]ragrance\)", "")
ingredientsDF.ingredient2 = ingredientsDF.ingredient2.str.strip(".* ")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^: "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^: ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\(.+\)$"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"[()]", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains("LAVENDAR"), "ingredient2"] = ingredientsDF.ingredient2.str.replace("LAVENDAR", "LAVENDER")
ingredientsDF = ingredientsDF.loc[ingredientsDF.ingredient2.str.len() > 1]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\(") & ingredientsDF.ingredient2.str.contains(r"\)"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"[()]", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^ORG "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^ORG ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^[Vv]irgin "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^[Vv]irgin ([Oo]rganic )?", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^(Wildcrafted|Wildflower) "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^(Wildcrafted|Wildflower) ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\[\+/-\]?:? "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\[\+/-\]?:? ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^\[\+/-.*[Mm]ay ?[Cc]ontain.*: "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^\[\+/-.*[Mm]ay ?[Cc]ontain.*: ", "")
ingredientsDF.loc[ingredientsDF.ingredient2 == "[+/-(May contain):Iron oxides CI 77492", "ingredient2"] = "CI 77492"
ingredientsDF.loc[ingredientsDF.ingredient2 == "[+/-:CI1 5850", "ingredient2"] = "CI 15850"
ingredientsDF.loc[ingredientsDF.ingredient2 == "(may contain): Mica", "ingredient2"] = "Mica"
ingredientsDF.ingredient2 = ingredientsDF.ingredient2.str.replace(r"^[-/]", "")
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^([Ff]lavor|[Ff]ran?gr[ae]nce)")]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^[Ll]aureth ?- ?\d"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^[Ll]aureth ?- ?(?=\d)", "Laureth-")

notIngredients = ["Matte Duchess", "Mattes:", "(^) DENOTES AN ECOCERT-APPROVED INGREDIENT",
                  "(^) Denotes an Ecocert approved ingredient", ") Essential Oil",
                  "LAVENDER MELT:", "100% Naturally Derived & Ecocertified",
                  "Herbal infusion", "Essence", "Essential", "LOVE!", "(C.I",
                  "Essential Oil & Phthalate Free Fragrance Oil Blend", "A)",
                  "Essential Oil (or Plant-based Scented Oil)", "+non-GMO",
                  "Essential Oil Blend of your choice", "+organic", "ACIDS"
                  "/ Organically Grown", "/PEUT CONTENIR", "Fawn Fantasia:",
                  "12 months", "aromatic isolates.]", "as well as lemon",
                  "60% Organic Material", "72hr Moisture", "Jubilee:", "black",
                  "(based on selection) Oil", "(listed above) Oil", "LOVE"
                  "(or Plant-based Scented Oil) Oil", "/ Organically Grown",
                  "/DÉRIVÉ DE L'HUILE DE FRAMBOISE", "Night Creature:", "Raw",
                  "Nightfall:", "No Ammonia", "Non-GMO", "Non-GMO Kosher",
                  "Non-GMO Verified", "Non-gmo ingredient", "Non-toxic",
                  "Non-photosensitizing", "Non-gmo", "non-GMO", "peace",
                  "non-paraben preservative", "organic", "passion", "older",
                  "Velouria:", "Velvet Eclipse:", "Vermillion Venom:",
                  "Wildcrafted", "Essence", "oxidizing", "Fair Trade", "ACIDS",
                  "99% Naturally Derived / 72% Organic", "Fairtrade",
                  " Organically Grown", "LOVE VIBRATIONS", "Certified Organic",
                  "LOOSE SETTING POWDER:", "Certified Ingredient", "Spoiled:",
                  "Certified Organic ** Natural", "Pantheon", "Vegan",
                  "Certified Organic / Certified Fair Trade", "Vegetable",
                  "Certified organic/Certifié biologique", "Up Side Brown",
                  "/ Certified Fair Trade", "Vegan Fragrance Oil Extract",
                  "Locks in the moisture", "listed above Oil", "local plants",
                  "Magic", "healing", "ª´Fair Trade Ingredient"
                  ]
ingredientsDF = ingredientsDF.query("ingredient2 != @notIngredients")

ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^(Nothing|Nude)")]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^[Nn]on-(GMO|gmo)"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^[Nn]on-(GMO|gmo) (& Sustainable )?", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"([Ff]ood [Gg]rade|FOOD GRADE) "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"([Ff]ood [Gg]rade|FOOD GRADE) ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^LOVE!.+[Oo]il( - |: )"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^LOVE!.+[Oo]il( - |: )", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"([Cc]old [Pp]ressed|COLD PRESSED) "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"([Cc]old [Pp]ressed|COLD PRESSED) ", "")
ingredientsDF.loc[ingredientsDF.ingredient2 == "CERTIFIED ORGANIC INGREDIENTS: Zingiber Officinale (Ginger) Root Oil", "ingredient2"] = "Zingiber Officinale Root Oil"
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^([Cc]ertified [Oo]rganic|CERTIFIED ORGANIC) ([Ii]ngredients?|Ingreident|INGREDIENT)")]
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^CERTIFIED ORGANIC")]
ingredientsDF = ingredientsDF.loc[~(ingredientsDF.ingredient2.str.contains(r"^([Cc]ertifie?d [Oo]rganic|CERTIFIED ORGANIC) ") & ingredientsDF.ingredient2.str.contains("GMO"))]
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^[Cc]ertifie?d [Oo]rganic "), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^[Cc]ertifie?d [Oo]rganic ", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^[Cc]etearyl [Aa]lcoh{1,2}ol"), "ingredient2"] = "Cetearyl Alcohol"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^Raw ((& )?Unrefined )?"), "ingredient2"] = ingredientsDF.ingredient2.str.replace(r"^[Rr]aw ((& )?Unrefined )?", "")
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"[Ss]qualane"), "ingredient2"] = "Squalane"
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"^Local [Pp]lants")]
ingredientsDF.loc[ingredientsDF.ingredient2 == "Local Beeswax", "ingredient2"] = "Beeswax"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^(GLYCERIN|[Gg]lycerin) \([a-zA-Z]"), "ingredient2"] = "Glycerin"
ingredientsDF.loc[ingredientsDF.ingredient2 == "Gly -cerin", "ingredient2"] = "Glycerin"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(r"^(GLYCERINE|Glycerine) \([a-zA-Z]"), "ingredient2"] = "Glycerine"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains('"', regex=False), "ingredient2"] = ingredientsDF.ingredient2.str.replace('"', '', regex=False)
ingredientsDF.loc[ingredientsDF.ingredient2 == "2-Hexanediol", "ingredient2"] = "1,2-Hexanediol"
ingredientsDF.loc[ingredientsDF.ingredient2 == "åÊ Carthamus Tinctorius (Safflower) Seed Oil", "ingredient2"] = "Safflower Seed Oil"
ingredientsDF.loc[ingredientsDF.ingredient2 == "or Ferric Ferrocyanide", "ingredient2"] = "Ferric Ferrocyanide"
ingredientsDF.ingredient2 = ingredientsDF.ingredient2.str.strip()

startingSubstringRemove2 = r"^(\^ ?([Dd]enotes|DENOTES|USDA|Contains|Fair|sustainably)|pH |other|[Pp]arfum|PARFUM|[Pp]araben|Organic(?!\w)|lots|love|liquid|VR [\s\w]+:|habitat|Ho )"
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(startingSubstringRemove2)]
startingSubstringRemove3 = r"^(we|or) "
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(startingSubstringRemove3)]
ingredientsDF = ingredientsDF.loc[~ingredientsDF.ingredient2.str.contains(r"Fair Trade Ingredient$")]

removeStartingSubstringOnly = r"^([Vv]egan |[Uu]nrefined (Certified |Organic |Raw )?|Untreated |Organic |This|[Vv]egetable (?![Oo]il|[Pp]rotein|[Pp]lacenta|[Ww]ax)|Liquid|MATTES?: )"
ingredientsDF.loc[ingredientsDF.ingredient2.str.contains(removeStartingSubstringOnly), "ingredient2"] = ingredientsDF.ingredient2.str.replace(removeStartingSubstringOnly, "")
ingredientsDF.ingredient2 = (ingredientsDF.ingredient2.str.strip()
                             .str.replace(r"(^[^\w]+|[^\w]+$)", "")
                             .str.strip()
                             )


ingredientsDF = ingredientsDF.drop_duplicates()

"""You know what, let's stop cleaning ingredient names here. Let's export the
data so I can start identifying ingredients later."""
splitIngredients = (splitIngredients.merge(ingredientsDF, "inner", "ingredient1")
                    .drop_duplicates()
                    )

"""Let's export an Excel file with the following tabs

- ReadMe
- 2022 products
- 2024 products
- original and cleaned ingredient lists
- ingredient lists and splitted ingredient names (uncleaned & cleaned names)
- splitted ingredient names and these names after further cleaning
- cleaned ingredient names splitted into 3 columns so they can be searched
using the CompTox batch search
"""
cleanedNames = (ingredientsDF.ingredient2.drop_duplicates().sort_values()
                .reset_index(drop=True)
                )
cleanedNames1 = cleanedNames.loc[:9999]
cleanedNames1 = (cleanedNames1.reset_index(drop=True)
                 .reset_index()
                 .rename(columns={"ingredient2": "cleanedNames1"})
                 )
cleanedNames2 = cleanedNames.loc[10000:19999]
cleanedNames2 = (cleanedNames2.reset_index(drop=True)
                 .reset_index()
                 .rename(columns={"ingredient2": "cleanedNames2"})
                 )
cleanedNames3 = cleanedNames.loc[20000:]
cleanedNames3 = (cleanedNames3.reset_index(drop=True)
                 .reset_index()
                 .rename(columns={"ingredient2": "cleanedNames3"})
                 )
cleanedNamesDF = (cleanedNames1.merge(cleanedNames2, "outer", "index")
                  .merge(cleanedNames3, "outer", "index")
                  .drop(columns=["index"])
                  )
# %%
note = ["The Breast Cancer Prevention Partners have a database of beauty and",
        "personal care products that are marketed to Black consumers, and",
        "have entered into a contract with DTSC where they provide DTSC a",
        "copy of this database. BCPP has a Red List of Chemicals with 3 Tiers",
        "and brands with products that contain chemicals from Tier 1 are deemed",
        "as not safe while brands without chemicals from Tier 1 are deemed as",
        "safer black beauty brands. I have taken this database, cleaned and",
        "separated the ingredient lists into individual ingredients as best",
        "as I could, and then cleaned the individual ingredient names as best",
        "as I could. This Excel file contains these products and their cleaned",
        "ingredient names. Because there is such a sheer amount of ingredient",
        "names (~22,000 cleaned names), there are likely ingredient names in",
        "here that I still need to clean, but I will not clean them anymore.",
        "The next task for me to do is to identify these ingredients.",
        "",
        "The original product entries from the tabs that were originally shown",
        "in the data that BCPP gave DTSC '2022 products' and '2024 products'.",
        "I also assigned each entry a product ID, but these product IDs do not",
        "correspond to the product IDs in the hidden tab of all products in the",
        "data that BCPP originally gave us. The tab 'Ingredient lists' contain",
        "the original ingredient lists from these 2 other tabs before and after",
        "I cleaned them. The tab 'List - ingredient combos' contains the",
        "cleaned ingredient lists their separated ingredients, including",
        "ingredient names from just after I cleaned the ingredient lists but",
        "before I cleaned individual ingredients and from after I cleaned the",
        "ingredient names. The tab 'Ingredients' list each ingredient before",
        "and after I cleaned them. In the tabs 'List - ingredient combos' and",
        "'Ingredients', the column 'ingredient1' contain ingredient names",
        "before they were cleaned while the column 'ingredient2' contain names",
        "after they were cleaned. Then I took all of the unique cleaned",
        "ingredient names and separated them into 3 columns of at most 10,000",
        "values each to facilitate copying and pasting them into the CompTox",
        "batch search to identify ingredients. The tab 'Cleaned ingredients'",
        "contain only clean ingredient names formatted into 3 columns like this."
        ]
readMe = pd.DataFrame({"Note": note})
exportPath = outputFolder/"Cleaned ingredient lists & ingredient names.xlsx"
if os.path.exists(exportPath) is False:
    with pd.ExcelWriter(exportPath) as w:
        readMe.to_excel(w, "ReadMe", index=False)
        data2022og.to_excel(w, "2022 products", index=False)
        data2024og.to_excel(w, "2024 products", index=False)
        ingredientLists.to_excel(w, "Ingredient lists", index=False)
        splitIngredients.to_excel(w, "List - ingredient combos", index=False)
        ingredientsDF.to_excel(w, "Ingredients", index=False)
        cleanedNamesDF.to_excel(w, "Cleaned ingredients", index=False)
