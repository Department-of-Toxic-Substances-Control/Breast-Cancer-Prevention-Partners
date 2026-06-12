# -*- coding: utf-8 -*-
"""
Created on Fri May 22 11:18:47 2026

@author: BChung

This script continues with fuzzy string matching to identify ingredients. I've
done strict matching with CompTox and CosIng and I've also finished fuzzy
string matching with CompTox. I also generated some preliminary combinations
for CosIng to start with fuzzy string matching with this database. This script
continues with fuzzy matching for CosIng.
"""
import os
from pathlib import Path
import pandas as pd
from fuzzyModule import fuzzy

repository = Path(os.getcwd())
repositoryFolder = Path(os.path.dirname(repository))
dataFolder = repositoryFolder/"Data"
inputFolder = dataFolder/"Input"
outputFolder = dataFolder/"Output"

fuzzyCompToxPath = outputFolder/"CompTox fuzzy.xlsx"
unidentifiedAfterCompTox = pd.read_excel(fuzzyCompToxPath, "Unidentified", dtype="string")
fuzzyCosIng1og = pd.read_excel(fuzzyCompToxPath, "Fuzzy CosIng", usecols=[0, 1, 2], dtype={"unidentified": "string", "identified": "string", "lengthsRatio": "float"})
fuzzyWrong = pd.read_excel(fuzzyCompToxPath, "Fuzzy wrong", dtype="string")

CosIngPath = inputFolder/"Cleaned CosIng database - scraped on January 21, 2026.xlsx"
CosIngOG = pd.read_excel(CosIngPath, "Substances", dtype="string")

plantsPath = inputFolder/"plantlst.txt"
plantsUSDAog = pd.read_csv(plantsPath)
"""This is the USDA's PLANTS database, which contains a large list of plants (I
don't know how comprehensive) and their scientific and common names, including
a single species that may have multiple scientific names as synonyms."""
# %%
plantsUSDA = plantsUSDAog.copy()
# plantsUSDA["scientificName"] = plantsUSDAog["Scientific Name with Author"].str.extract(r"(\w+ \w+)")
plantsScientificNameSplit = plantsUSDA["Scientific Name with Author"].str.split(" ", expand=True)
plantsScientificNameSplit = (plantsScientificNameSplit.filter([0, 1])
                             .rename(columns={0: "genus", 1: "species"})
                             )
plantsUSDA = plantsUSDA.join(plantsScientificNameSplit)
plantsUSDA["speciesName"] = plantsUSDA.genus + " " + plantsUSDA.species
plantsUSDA.speciesName = plantsUSDA.speciesName.str.upper()

plantNames = plantsUSDA.speciesName.drop_duplicates().tolist()
# %%
"""I'm gonna remove INCI ingredients with multiple plant species names"""
CosIng = CosIngOG.copy()
CosIng["nameLength"] = CosIng.INCI.str.len()
CosIng["speciesCount"] = 0

for plantName in plantNames:
    CosIng.loc[CosIng.INCI.str.contains(plantName, regex=False), "speciesCount"] = CosIng.speciesCount + 1
CosIng.loc[CosIng.INCI.str.contains("/", regex=False), "forwardSlash"] = True
CosIng.loc[~CosIng.INCI.str.contains("/", regex=False), "forwardSlash"] = False
multiplePlantNames = CosIng.loc[(CosIng.speciesCount >= 2) & (CosIng.forwardSlash == True) & (CosIng.CASRN == "-"), "INCI"].drop_duplicates().tolist()
# %%
fuzzyCosIng1 = fuzzyCosIng1og.copy()
fuzzyCosIng1["unidentifiedLength"] = fuzzyCosIng1.unidentified.str.len()
fuzzyCosIng1["identifiedLength"] = fuzzyCosIng1.identified.str.len()
notIngredients = ["CI", "ST", "EXT", "OIL", "RED", "SIS", "EXTRACT", "FLOWER",
                  "ACTING AS A BRIGHTENING AGENT", "COPOLYMER", "HYDROGENATED",
                  "FRUIT EXTRACT", "FLOWER WATER", "BLUE", "VIOLET", "CETYL",
                  "ABSOLUTE", "FLOWER OIL", "LYE", "CROSSPOLYMER", "PIGMENT",
                  "BARK EXTRACT", "GUM", "COLOR", "DENAT", "SCENT", "AFRICAN",
                  "NATURAL", "HYDROXYETHYL", "ROSA", "VINYL", "AROMA", "LAKE",
                  "BUTTER", "HOUSTON", "EMERALD ECLIPSE", "MORE", "CETEARYL",
                  "LACTOBACILLUS", "HEIR", "PALMITOYL", "LIGHT", "SUNSET",
                  "BLUE NO", "CETEARAMIDOETHYL", "EVENING", "VEG", "VEGETABLE",
                  "HYDROXYSTEARIC", "SYNTHETIC", "GLUCOSIDE", "TRISODIUM",
                  "TYRENE COPOLYMER", "CORN", "JUNIPERUS", "GARDENIA", "OLEIC",
                  "DISUCCINATE", "PENTACLETHRA", "COCO", "EXTRACTS", "DULCIS",
                  "HYDROGENATED POLYDECANE", "LINOLENIC", "HCI",
                  ]

wrongUnidentified = ["ABYSSINIAN", "ACRYLATES", "AGAVE"]
fuzzyCosIng1wrong1 = fuzzyCosIng1.loc[fuzzyCosIng1.unidentified.isin(notIngredients + wrongUnidentified)]

fuzzyCosIng1 = fuzzyCosIng1.loc[~fuzzyCosIng1.unidentified.isin(notIngredients)]
# fuzzyCosIng1 = fuzzyCosIng1.loc[fuzzyCosIng1.unidentified.isin(notIngredients)]
fuzzyCosIng1.loc[fuzzyCosIng1.unidentified.isin(wrongUnidentified), "match"] = False

fuzzyCosIng1 = fuzzyCosIng1.loc[~fuzzyCosIng1.identified.isin(multiplePlantNames)]

fuzzyCosIng1matchCounts = (fuzzyCosIng1.groupby("unidentified")["identified"].count()
                           .reset_index()
                           .rename(columns={"identified": "identifiedMatches"})
                           )
fuzzyCosIng1 = fuzzyCosIng1.merge(fuzzyCosIng1matchCounts, "left", "unidentified")

fuzzyCosIng1identifiedGroup = (fuzzyCosIng1.groupby("identified")["unidentified"].count()
                               .reset_index()
                               .rename(columns={"unidentified": "potentialMatches"})
                               )

willNeverMatch = ["PPG-1", "POLYGLYCERYL-3", "RICE", "LIMONE", "NUTMEG",
                  "ARTEMISIA", "ASTRAGALUS", "BIS-VINYL DIMETHICONE COPOLYMER",
                  "ECHINACEA", "LEATHER", "PLUMERIA", "LAVENDER FLOWER",
                  "YARROW", "LEXFEEL WOW-A",
                  ]
willNeverMatchRegex = r"SH-(OLIGO|POLY)PEPTIDE-1|(GLYCERETH|OCTAPEPTIDE)-2"
"""These are unidentified ingredients that will never be matched because I'm
pretty sure that CosIng doesn't have them. For example, the name 'SH-OLIGOPEPTIDE-1'
is matched to ingredient names on CosIng that contain all of that plus another
digit after the 1, e.g. names that end with 13, 10, 11, 15, and 14 on CosIng.
"""
wrongIdentified = ["MILK", "MEL", "RNA", "TIN", "FICIN", "ARGON", "CREAM",
                   "ACETUM", "AGAR", "AROMA", "BETAINE", "BUTTER", "CARBON",
                   "CHROMIUM", "DIACETYL", "EEL EXTRACT", "EMERALD", "ETHANE",
                   "ETHYL ACRYLATE", "HEXENE", "HEXYLGLYCERIN", "HYDROGEN",
                   "IONONE", "ISATIN", "ISOCETYL BEHENATE", "LYCOPENE", "UREA",
                   "MEA-DICETEARYL PHOSPHATE", "MYRETH-10", "NERAL", "PARFUM",
                   "MORINGA OIL/HYDROGENATED MORINGA OIL ESTERS", "PENTANE",
                   "PALMITOYL TRIPEPTIDE-5", "PENTAERYTHRITOL", "PHENOL",
                   "PROPANE", "PROPIONALDEHYDE", "SACCHARIN", "STEARAMIDE",
                   "STYRENE", "UMBER", "UNDECYLENIC ACID", "VINYL DIMETHICONE",
                   "XYLENE", "BETA-ALANINE", "BAMBOO VINEGAR", "GLYCOL"
                   ]
"""These are CosIng ingredients that will be matched by fuzzy string matching
but will never be actual true matches."""
fuzzyCosIng1 = fuzzyCosIng1.loc[~(fuzzyCosIng1.unidentified.str.contains(willNeverMatchRegex) | fuzzyCosIng1.unidentified.isin(willNeverMatch) | fuzzyCosIng1.identified.isin(wrongIdentified))]
CosIng = CosIng.loc[~CosIng.INCI.isin(multiplePlantNames + wrongIdentified)]


identifiedRight = ["ACACIA CONCINNA FRUIT EXTRACT", "ACHILLEA MILLEFOLIUM OIL",
                   "AMINOMETHYL PROPANOL", "BEER", "BEESWAX", "BENZOPHENONE",
                   "BENZOYL PEROXIDE", "BHT", "BOSWELLIA CARTERII OIL", "WOOL",
                   "BUTANE", "BUTYLENE GLYCOL", "BUTYLPHENYL METHYLPROPIONAL",
                   "CADINENE", "CANDELILLA CERA", "CANNABIGEROL", "CAPRAE LAC",
                   "CITRINE", "FUCOIDAN", "FULLER'S EARTH", "GERANIOL",
                   "GLYCERYL ISOSTEARATE", "HEXYL BENZOATE", "LAPIS LAZULI",
                   "HYDROCORTISONE ACETATE", "LAURYL BETAINE", "LINALOOL",
                   "HYDROGENATED CASTOR OIL BEHENYL ESTERS", "MENTHOL",
                   "LANTANA CAMARA LEAF EXTRACT", "OCTYLDODECANOL", "OLETH-20",
                   "OENOTHERA BIENNIS OIL", "OLIBANUM", "SAPONINS", "SULFUR",
                   "PEG/PPG-17/18 DIMETHICONE", "SODIUM PCA", "SOLUM FULLONUM",
                   "SPINACIA OLERACEA", "SUNSET YELLOW", "YEAST",
                   "UNDARIA PINNATIFIDA CELL CULTURE EXTRACT", "XANTHAN GUM",
                   "VIOLA TRICOLOR EXTRACT", "YUCCA GLAUCA ROOT EXTRACT",
                   "ZINC GLUCONATE", "ACETYL ZINGERONE",
                   "ACRYLIC ACID/ACRYLAMIDOMETHYL PROPANE SULFONIC ACID COPOLYMER",
                   "APIS MELLIFERA (BEES) HONEY ABSOLUTE",
                   "BUTYROSPERMUM PARKII BUTTER",
                   "CARTHAMUS TINCTORIUS OLEOSOMES",
                   "CITRUS AURANTIFOLIA PEEL WATER",
                   "CITRUS GRANDIS SEED EXTRACT"
                   ]
identifiedRightRegex = r"^(C(12-15|14-22)|CANANGA|CAPRYLHYDROXAMIC|CETYL ([EH]|PEG/PPG-10)|CYMBOPOGON|DAUCUS|DI-PPG|GERANIUM|HYDRANGEA|HYDROCORTISONE|LINUM|METHYL GLUCETH|MONARDA|MYR(ICA|OXYLON)|NYMPHAEA|ONOPORDUM|PEARL|POLIANTHES|PRAMOXINE|CI 7728[89])"
unidentifiedRight = ["ADANSONIA DIGITATA SEED OIL (PURE BAOBAB OIL) & WILD NORTHERN HONEY",
                     "ALKYL BENZOATE", "AMINOMETHYL PROPANOL POLYACRYLATE-3",
                     "BRASSICA NAPUS OIL ROSMARINUS OFFICINALIS EXTRACT",
                     "BUTYLENE GLYCOL (HYDRATING", "CARICA PAPAYA",
                     "CAMELLIA (CAMELLIA OLEIFERA SEED OIL",
                     "CAPROOYL PHYTOSPHINGOSINE;CAPROOYL SPHINGOSINE",
                     "CARDIOSPERMUM HALICACABUM", "CI 77492,CI 77499",
                     "CHAMOMILLA RECUTITA FLOWER OIL & MEL EXTRACT",
                     "CHLOROPHYLLIN-COPPER COMPLEX (CI 75810",
                     "CI 60725VIOLET 2RELAXER BASE INGREDIENTS AQUA",
                     "CITRUS AURANTIUM BERGAMIA PEEL OIL (BERGAPTENE FREE) & POGOSTEMON CABLIN LEAF OIL",
                     "CYMBOPOGON SCHOENANTHUS OIL & CAMELLIA OLEIFERA SEED OIL",
                     "DAUCUS CAROTA SATIVA SEED OIL & BOSWELLIA CARTERII OIL",
                     "DISTILLED AQUA INFUSED WITH ORGANIC URTICA DIOICA (NETTLE LEAF",
                     "E FARNESOL WHICH IMPARTS ANTI-MICROBIAL PROPERTIES AS WELL AS ALPHA-BISABOLOL A WELL ESTABLISHED ANTI-INFLAMMATORY CONSTITUENT",
                     "ETHYLHEXYL PALMITATE & LACTOBACILLUS/LEMON PEEL FERMENT EXTRACT",
                     "FD&AMP;C RED NO.40 AL LAKE (CI 16035)，FD&AMP;C BLUE NO.1 AL LAKE (CI 42090",
                     "GLYCERIN & WATER & OPUNTIA TUNA FLOWER/STEM EXTRACT HYLOCEREUS UNDATUS (DRAGON) FRUIT EXTRACT",
                     "GLYCOSYL TREHALOSE HYDROGENATED STARCH HYDROLYSATE",
                     "HELIANTHUS ANNUUS (SUNFLOWER) SEED OIL & VACCINIUM MYRTILLUS FRUIT/LEAF EXTRACT & SACCHARUM OFFICINARUM (SUGAR CANE) EXTRACT & CITRUS AURANTIUM DULCIS (ORANGE) FRUIT EXTRACT & CITRUS LIMON (LEMON) FRUIT EXTRACT ACER SACCHARUM (SUGAR MAPLE) EXTRACT LACTOBACILLUS/SALIX ALBA BARK FERMENT FILTRATE",
                     "HYDROLYZED WHEAT PROTEIN (AND) HYDROLYZED WHEAT STARCH",
                     "INFUSED WITH ROSA CANINA SEED (ROSEHIP) & CALENDULA OFFICINALIS FLOWER (CALENDULA",
                     "IRON OXIDES (CI 77491 / CI 77492 / CI 77499",
                     "IRON OXIDES (CI 77491/CI 77492/CI 77499",
                     "IRON OXIDES (CI 77491/CI 77492/CI77499",
                     "IRON OXIDES (CI 77492) IRON OXIDES (CI 77499",
                     "LINALOOL GERANIOL BORNYL ACETATE",
                     "MARULA OIL ( SCLEROCARYA BIRREA SEED OIL) CLARY SAGE (SALVIA SCLAREA OIL",
                     "MYRISTOYL HEXAPEPTIDE-16,URTICA DIOICA (NETTLE) EXTRACT",
                     "NATURALLY OCCURRING CONSTITUENTS OF ORGANIC VANILLA: VANILLIN WITH TRACES OF EUGENOL",
                     "OLETH-20,PEG-12 DIMETHICONE",
                     "OLIGOPEPTIDE-51,LITHOSPERMUM ERYTHRORHIZON ROOT EXTRACT",
                     "ORYZA SATIVA (RICE) EXTRACT & WATER & EUPHRASIA OFFICINALIS EXTRACT",
                     "PHENOXYETHANOL (AND) CAPRYLYL GLUCOL (AND) SORBIC ACID",
                     "POGOSTEMON CABLIN LEAF OIL & BOSWELLIA CARTERII OIL",
                     "POGOSTEMON CABLIN OIL (PATCHOULI) & CANANGA ODORATA FLOWER OIL (YLANG YLANG) OIL",
                     "SOLUM FULLONUM (FULLER'S EARTH",
                     "TALC ORYZA SATIVA (RICE) EXTRACT",
                     "TRIETHOXYCAPRYLYLSILANE IRON OXIDES (CI 77492",
                     "WATER & CAPRYLYL GLYCOL & HEXYLENE GLYCOL & WASABIA JAPONICA (WASABI) ROOT EXTRACT & ZINGIBER OFFICINALE (GINGER) ROOT EXTRACT & ALLIUM SATIVUM (GARLIC) BULB EXTRACT",
                     "WATER & VACCINIUM MYRTILLUS FRUIT/LEAF EXTRACT & SACCHARUM OFFICINARUM (SUGAR CANE) EXTRACT",
                     "SODIUM METHYL 2-SULFOLAURATE & DISODIUM 2-SULFOLAURATE",
                     "D-ALPHA-TOCOPHERYL ACETATE HELIANTHUS ANNUUS (SUNFLOWER) SEED OIL & VACCINIUM MYRTILLUS FRUIT/LEAF EXTRACT & SACCHARUM OFFICINARUM (SUGAR CANE) EXTRACT & CITRUS AURANTIUM DULCIS (ORANGE) FRUIT EXTRACT & CITRUS LIMON (LEMON) FRUIT EXTRACT & ACER SACCHARUM (SUGAR MAPLE) EXTRACT",
                     "CUCUMIS SATIVUS (CUCUMBER) FRUIT EXTRACT OPUNTIA FICUS-INDICA PADDLE (PRICKLY PEAR CACTUS) EXTRACT ASCORBIC ACID",
                     
                     ]
identifiedWrong = ["ACETYLPHYTOSPHINGOSINE", "BUTTER EXTRACT", "COCAMIDE",
                   "DEXTRIN", "EGG", "CACTUS", "FURFURAL", "GLYCERETH-8",
                   "HYDROXYBENZALDEHYDE", "MANGO SEED OIL PEG-70 ESTERS",
                   "ISOPENTENONE BUTYLENEGLYCOL CYCLIC ACETAL", "MEK", "ZINC",
                   "PALM KERNEL/COCO GLUCOSIDE", "PG-AMODIMETHICONE", "ALGAE",
                   "POLY(C30-45 OLEFIN)", "PPG-10 CETYL ETHER PHOSPHATE",
                   "PROPYL ALCOHOL", "PUMPKIN FRUIT EXTRACT BETA-GLUCAN",
                   "SEBACIC ACID", "SUS EXTRACT", "ACRYLATES CROSSPOLYMER",
                   "CAMELINA SEED OIL GLYCERETH-8 ESTERS", "CETETH-2",
                   "CETYL KOMBO BUTTERATE"]
identifiedWrongRegex = r"^BROCCOLI"
unidentifiedWrong = ["ARACHIDYL", "BIS-DIGLYCERYL", "BLACK CURRANT SEED OIL"
                     "BIS-VINYL DIMETHICONE COPOLYMER", "BROWN RICE", "CHERRY",
                     "BUTROSPERMUM PARKII (PEG 75 SHEA BUTTER GLYCERIDE",
                     "BUTYLPHENYL METHYL PROPIONATE", "MENTHA", "GRAPESEED",
                     "CAMELLIA ASSAMICA (BLACK ASSAM) TEA EXTRACT", "ISONONYL",
                     "CETYL TRIETHYLMONIUM OLIVATE DIMETHICONE PEG-8 SUCCINATE",
                     "SAPONIFIED OILS OF: OLEA EUROPAEA (OLIVE) COCOS NUCIFERA (COCONUT) BUTYROSPERMUM PARKII (UNREFINED SHEA BUTTER) APPLE CIDER VINEGAR",
                     "TANACETUM ANNUUM (BLUE TANSY) FLORAL WATER", "SILYLATE",
                     "TRIMETHYLPENTANEDIOL/ADIPIC ACID/GLYCERIN CROSSPOLYMER",
                     "WATERMELON SEED OIL", "WATERMELON SEED", "MEADOWFOAM",
                     "COCAMIDOPROPYLTRIMONIUM DIMETHICONE PEG-8 SUCCINATE",
                     "SILSESQUIOXANE CROSSPOLYMER", "CYCLAMEN", "OAT MILK",
                     "METHOSULFATE", "LINOLEIC", "SPIRULINA", "RHODIOLA",
                     "PEG-75 SHEA BUTTER GLYCERIDE (BUTYROSPERUM PARKII",
                     "PEG-75) SHEA BUTTER GLYCERIDE (BUTYROSPERMUM PARKII",
                     "POMEGRANATE FRUIT", "HYDROGENATED DIDECENE", "NEROLI",
                     "CAPSICUM", "HIBISCUS", "HAMAMELIS", "JATAMANSI", "PLUM",
                     "JUNIPER", "COCONUT FRUIT", "PINEAPPLE", "XANTHUM",
                     "HYDROGENATED CASTER OIL/SEBACIC ACID COPOLYMER (CASTOR OIL) STYRENE/ACRYLATES COPOLYMER",
                     "ROSEMARY LEAF", "CYSTINE BIS-PG PROPYL SILANTRIOL",
                     "TANGERINE", "E WAX", "CLOVER",
                     "COCAMIDOPROPYL TRIMETHYL AMMONIUM CHLORIDE AND PEG-8 DIMETHICONE SUCCINATE",
                     "IPDI/DI-C12-13 ALKYL TARTRATE/BIS- HYDROXYETHOXYPROPYL DIMETHICONE COPOLYMER",
                     "HYDROXYLPHENYL PROPAMIDOBENZOIC ACID ROSMARINUS OFFICINALIS (ROSEMARY) LEAF EXTRACT",
                     "ACRYLATES/PEG-10 MALEATE/STYRENE COPOLYMER", "MINT",
                     "DISODIUM EDTA COPPER (HYDRATING", "FIR", "ZEA MAYS",
                     "COCODIMONIUM HYDROXYPROPYL HYDROLYZED (HUMAN) HAIR KERATIN",
                     "HYDROGENATED STYRENE/METHYL STYRENE/INDENE COPOLYMER",
                     
                     ]
unidentifiedWrongRegex = r"^(CITRULLUS|PLANT KERATIN|STYRENE.*ACRYLATES COPOLYMER$)"
fuzzyCosIng1.loc[fuzzyCosIng1.identified.isin(identifiedRight)| fuzzyCosIng1.identified.str.contains(identifiedRightRegex) | fuzzyCosIng1.unidentified.isin(unidentifiedRight), "match"] = True
fuzzyCosIng1.loc[fuzzyCosIng1.identified.isin(identifiedWrong) | fuzzyCosIng1.identified.str.contains(identifiedWrongRegex) | fuzzyCosIng1.unidentified.isin(unidentifiedWrong) | fuzzyCosIng1.unidentified.str.contains(unidentifiedWrongRegex), "match"] = False
identifiedIncludeDict = {"ACETYL GLUCOSAMINE": "N-ACETYL GLUCOSAMINE",
                         "MICA": r"\bMICA\b",
                         "CAMELLIA JAPONICA SEED OIL": r"\bTSUB[AU]KI\b",
                         "ETHYLHEXYL HYDROXYSTEARATE": r"^(ETHYLHEXYL )?HYDROXYSTEARATE"}
for identified, substring in identifiedIncludeDict.items():
    fuzzyCosIng1.loc[(fuzzyCosIng1.identified == identified) & fuzzyCosIng1.unidentified.str.contains(substring), "match"] = True
    fuzzyCosIng1.loc[(fuzzyCosIng1.identified == identified) & ~fuzzyCosIng1.unidentified.str.contains(substring), "match"] = False

unidentifiedIncludeDict = {"3-CYCLOHEXENE CARBOXALDEHYDE": "HYDROXYISOHEXYL 3-CYCLOHEXENE CARBOXALDEHYDE",
                           "ACRYLAMIDOPROPYLTRIMONIUM": "ACRYLAMIDOPROPYLTRIMONIUM CHLORIDE/ACRYLAMIDE COPOLYMER",
                           "ACRYLAMIDOPROPYLTRIMONIUM CHLORIDE": "ACRYLAMIDE COPOLYMER",
                           "ACRYLATE CROSSPOLYMER": r"^ACRYLATES/C10-30 ALKYL ACRYLATE CROSSPOLYMER$",
                           "ACRYLATES/BEHENETH-25": "ACRYLATES/BEHENETH-25 METHACRYLATE COPOLYMER",
                           "ACRYLATES/C10-30": r"^ACRYLATES/C10-30 ALKYL ACRYLATE CROSSPOLYMER$",
                           "ACRYLATES/C10-30 ALKYL": r"^ACRYLATES/C10-30 ALKYL ACRYLATE CROSSPOLYMER$",
                           "ACRYLATES/DIMETHYLAMINOETHYL METHACRYLATE": "ACRYLATES/DIMETHYLAMINOETHYL METHACRYLATE COPOLYMER",
                           "ACRYLATES/STEARETH-20": "ACRYLATES/STEARETH-20 METHACRYLATE COPOLYMER",
                           "ACRYLOYLDIMETHYL TAURATE COPOLYMER": "HYDROXYETHYL",
                           "ACTINIDIA": "ACTINIDIA CHINENSIS FRUIT",
                           "ADANSONIA": r"^ADANSONIA DIGITATA SEED OIL$",
                           "ADANSONIA DIGITATA SEED": r"^ADANSONIA DIGITATA SEED OIL$",
                           "ADIPIC ACID/NEOPENTYL": "ADIPIC ACID/NEOPENTYL GLYCOL/TRIMELLITIC ANHYDRIDE COPOLYMER",
                           "ADIPIC ACID/NEOPENTYL GLYCOL/TRIMELLITIC ANHYDRIDE": "ADIPIC ACID/NEOPENTYL GLYCOL/TRIMELLITIC ANHYDRIDE COPOLYMER",
                           "AGAVE AMERICANA": "LEAF EXTRACT",
                           "ALKYL ACRYLATE CROSSPOLYMER": r"^ACRYLATES/C10-30 ALKYL ACRYLATE CROSSPOLYMER$",
                           "ALKYL DIMETHICONE": r"^C30-45 ALKYL DIMETHICONE$",
                           "AMARANTHUS CAUDATUS": "AMARANTHUS CAUDATUS EXTRACT",
                           "AMELLIA SINENSIS LEAF EXTRACT": r"^CAMELLIA SINENSIS LEAF EXTRACT$",
                           "AMINO ACID BLEND(SODIUM PCA": "SODIUM PCA",
                           "AMINOMETHYL PROPANOL (WATER SOLUBLE": "AMINOMETHYL PROPANOL",
                           "AMYRIS BALSAMIFERA": "BARK OIL",
                           "AMYRIS BALSAMIFERA BARK OIL (SANDALWOOD": "AMYRIS BALSAMIFERA BARK OIL",
                           "ANANAS SATIVUS": "FRUIT EXTRACT",
                           "ANANAS SATIVUS FRUIT EXTRACT (PINEAPPLE ENZYME": "ANANAS SATIVUS FRUIT EXTRACT",
                           "ANHYDRIDE COPOLYMER": r"^ADIPIC ACID/NEOPENTYL GLYCOL/TRIMELLITIC ANHYDRIDE COPOLYMER$",
                           "ANTHEMIS NOBILIS FLOWER WATER (CHAMOMILE HYDROSOL": "ANTHEMIS NOBILIS FLOWER WATER",                           
                           "ARCTIUM LAPPA": "ROOT EXTRACT",
                           "ARGIRELINE(ACETYL HEXAPEPTIDE-8": "ACETYL HEXAPEPTIDE-8",
                           "ASCOPHYLLUM NODOSUM (ALGAE) EXTRACT": "ASCOPHYLLUM NODOSUM",
                           "ASTRAL ROSE ORCHID: CALCIUM TITANIUM BOROSILICATE": "CALCIUM TITANIUM BOROSILICATE",
                           "AUSTRALIAN SANDALWOOD - ALPHA-SANTALOL & BETA-SANTALOL": r"(ALPHA|BETA)-SANTALOL",
                           "AVENA SATIVA": "AVENA SATIVA KERNEL EXTRACT",
                           "AVENA SATIVA (OAT) KERNEL EXTRACT PEG-60 HYDROGENATED CASTOR OIL": "CASTOR OIL",
                           "AVENA SATIVA MEAL EXTRACT ( OAT SILK": "AVENA SATIVA MEAL EXTRACT",
                           "BACILLUS FERMENT": r"^BACILLUS FERMENT",
                           "BENZOTRIAZOLYL": "SULFONATE",
                           "BETA VULGARIS": "BETA VULGARIS EXTRACT",
                           "BOROSILICATE": "CALCIUM SODIUM",
                           "BRANCH/FRUIT/LEAF EXTRACT": "NEPHELIUM",
                           "BRASSICA": r"^BRASSICA OLERACEA (GEMMIFERA|ITALICA) EXTRACT$",
                           "BURSERA FAGAROIDES WOOD OIL (LINALOE": "BURSERA FAGAROIDES WOOD OIL",
                           "C10-40": "ISOALKYLAMIDOPROPYLETHYLDIMONIUM",
                           "C12-15 ALKYL": "BENZOATE",
                           "CACAO": "SEED EXTRACT",
                           "CAESALPINIA": r"^CAESALPINIA SPINOSA GUM$",
                           "CALCIUM SODIUM": "BOROSILICATE",
                           "CALCIUM SODIUM PHOSPHOSILICATE & MICA/CI N/A & 70019": r"^(CALCIUM SODIUM PHOSPHOSILICATE|MICA)$",
                           "CALOPHYLLUM INOPHYLLUM": r"^CALOPHYLLUM INOPHYLLUM SEED OIL$",
                           "CAMELLIA": r"^CAMELLIA OLEIFERA SEED OIL$",
                           "CAMELLIA OLEIFERA": "LEAF EXTRACT",
                           "CAPRYLATE/CAPRATE": r"^COCO-CAPRYLATE/CAPRATE$",
                           "CAPRYLOYL": r"^CAPRYLOYL GLYCERIN/SEBACIC ACID COPOLYMER$",
                           "CAPRYLOYL GLYCERIN/SEBACIC ACID COPOLYMER (NATURALLY DERIVED FROM COCONUT": "CAPRYLOYL GLYCERIN/SEBACIC ACID COPOLYMER",
                           "CAPRYLOYOL GLYCERIN/SEBACIC ACID COPOLYMER OPUNTIA FICUS-INDICA FLOWER EXTRACT (PRICKLY PEAR": "OPUNTIA FICUS-INDICA FLOWER EXTRACT",
                           "CARBOXYLIC ACID": "TRIDECETH-7 CARBOXYLIC ACID",
                           "CARDAMOM": "SEED OIL",
                           "CARICA PAPAYA FRUIT EXTRACT (PAPAYA": "CARICA PAPAYA FRUIT EXTRACT",
                           "CARTHAMUS TINCTORIUS (SAFFLOWER) SEED OIL & PANAX GINSENG ROOT EXTRACT": "PANAX GINSENG ROOT EXTRACT",
                           "CARYODENDRON": "SEED OIL",
                           "CENTAUREA CYANUS FLOWER EXTRACT (SOOTHING": "CENTAUREA CYANUS FLOWER EXTRACT",
                           "CHAMOMILLA": "CHAMOMILLA RECUTITA FLOWER EXTRACT",
                           "CHAMOMILLA RECUTITA FLOWER": "CHAMOMILLA RECUTITA FLOWER EXTRACT",
                           "CHINENSIS": r"^SIMMONDSIA CHINENSIS SEED",
                           "CHONDRUS CRISPUS (ALGAE) EXTRACT": "CHONDRUS CRISPUS",
                           "CITRIC PEG-12 DIMETHICONE HYDOOLYZED WHEAT PROTEIN": "PEG-12 DIMETHICONE",
                           "IPDI/DI-C12-13 ALKYL TARTRATE/BIS-HYDROXYETHOXYPROPYL DIMETHICONE COPOLYMER BUTYLENE GLYCOL DICAPRYLATE/DICAPRATE": r"^(IPDI/DI-C12-13 ALKYL TARTRATE/BIS-HYDROXYETHOXYPROPYL DIMETHICONE COPOLYMER|BUTYLENE GLYCOL DICAPRYLATE/DICAPRATE)$",
                           "LACTOBACILLUS/TOMATO FRUIT FERMENT EXTRACT & ORYZA SATIVA (RICE) EXTRACT & KERATIN AMINO ACIDS & ACYL COENZYME A DESATURASE": r"^(LACTOBACILLUS/TOMATO FRUIT FERMENT EXTRACT|ORYZA SATIVA|KERATIN AMINO ACIDS|ACYL COENZYME A DESATURASE)$",
                           "OLIGOPEPTIDE-68,CRAMBE ABYSSINICA SEED OIL": r"^(CRAMBE ABYSSINICA SEED OIL|OLIGOPEPTIDE-68)$",
                           "RICINUS COMMUNIS (CASTOR OIL) OLEA EUROPAEA FRUIT OIL (OLIVE OIL) ROSMARINUS OFFICINALIS LEAF OIL (ROSEMARY VERBENINE OIL": r"^(OLEA EUROPAEA FRUIT OIL|ROSMARINUS OFFICINALIS LEAF OIL)$",
                           "SERICA POWDER / SILK POWDER / POUDRE DE SOIE": r"^(SERICA|SILK) POWDER$",
                           "T-BUTYL HYDROXYHYDROCINNAMATE": "PENTAERYTHRITYL",
                           "ALOE BARBADENSIS LEAF WATER (AND) CITRUS LIMON (LEMON) PEEL WATER (AND) MORINGA OLEIFERA LEAF WATER (AND) GLUCONOLACTONE (AND) SODIUM BENZOATE": r"^(ALOE BARBADENSIS LEAF WATER|SODIUM BENZOATE|GLUCONOLACTONE)$",
                           "WATER & CHONDRUS CRISPUS EXTRACT": r"^(WATER|CHONDRUS CRISPUS EXTRACT)$",
                           "PALM KERNELATE": r"^POTASSIUM",
                           "OLIVE LEAF EXTRACT": "HYDROLYZED OLIVE LEAF EXTRACT",
                           "PG-PROPYL SILANETRIOL": "HYDROLYZED VEGETABLE PROTEIN PG-PROPYL SILANETRIOL",
                           "TETRAMETHYL": "TETRAMETHYL ACETYLOCTAHYDRONAPHTHALENES",
                           "HYDROXYETHYL METHACRYLATE (HEMA": r"^HEMA$",
                           "RASPBERRY": r"^HYDROLYZED RASPBERRY FRUIT$",
                           "PEANUT": r"^PEANUT (ACID|GLYCERIDES)",
                           "HYDROXYHYDROCINNAMATE": r"TETRA-DI-T-BUTYL",
                           "POLYGLYCERIDES": r"(?<!SOY )POLYGLYCERIDES$",
                           "STEAROYL": r"^STEAROYL GLUTAMIC ACID$",
                           "SOY PROTEIN": r"^HYDROLYZED SOY PROTEIN$",
                           "MORINGA": r"^MORINGA OLEIFERA",
                           "LAVANDULA": r"^LAVANDULA ANGUSTIFOLIA OIL$",
                           "LEUCONOSTOC": r"^LEUCONOSTOC/RADISH ROOT FERMENT FILTRATE$",
                           "TRISODIUM ETHYLENEDIAMINE": "TRISODIUM ETHYLENEDIAMINE DISUCCINATE",
                           "GLYCERIN & WATER & GYNOSTEMMA PENTAPHYLLUM EXTRACT & PANAX GINSENG ROOT EXTRACT": r"^((PANAX GINSENG ROOT|GYNOSTEMMA PENTAPHYLLUM) EXTRACT|GLYCERIN|WATER)$",
                           "MATRICARIA": "RECUTITA",
                           "OLIVE LEAF": r"^HYDROLYZED OLIVE LEAF EXTRACT$",
                           "SALVIA": "SALVIA OFFICINALIS OIL",
                           "EUGENIA": "EUGENIA CARYOPHYLLUS FLOWER OIL",
                           "MORINDA": "MORINDA CITRIFOLIA FRUIT EXTRACT",
                           "HYACINTH": "HYACINTHUS ORIENTALIS EXTRACT"
                           }
for unidentified, substring in unidentifiedIncludeDict.items():
    fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified == unidentified) & fuzzyCosIng1.identified.str.contains(substring), "match"] = True
    fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified == unidentified) & ~fuzzyCosIng1.identified.str.contains(substring), "match"] = False

identifiedExtract = ["AMORPHOPHALLUS KONJAC", "ANGELICA ROOT",
                     "ANTHEMIS NOBILIS", "ASPALATHUS LINEARIS LEAF",
                     "ASTRAGALUS MEMBRANACEUS",
                     "AVERRHOA CARAMBOLA FRUIT EXTRACT & PASSIFLORA INCARNATA FRUIT EXTRACT & ACTINIDIA CHINENSIS (KIWI) FRUIT EXTRACT & GARCINIA MANGOSTANA EXTRACT & ANANAS SATIVUS (PINEAPPLE) FRUIT EXTRACT & PUNICA GRANATUM EXTRACT & LITCHI CHINENSIS FRUIT EXTRACT & ZIZYPHUS JUJUBE FRUIT EXTRACT & PSIDIUM GUAJAVA FRUIT EXTRACT",
                     "BEE POLLEN EXTRACT", "PEAT"]
for unidentified in identifiedExtract:
    fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified == unidentified) & fuzzyCosIng1.identified.str.contains("EXTRACT"), "match"] = True
    fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified == unidentified) & ~fuzzyCosIng1.identified.str.contains("EXTRACT"), "match"] = False


identifiedExcludeDict = {"HONEY": r"(HONEY ?(SUCKLE|DEW|BUSH)|FERMENT)",
                         "VITIS VINIFERA": "OIL",
                         "ALCOHOL": "ALCOHOL FREE"}
for identified, substring in identifiedExcludeDict.items():
    fuzzyCosIng1.loc[(fuzzyCosIng1.identified == identified) & fuzzyCosIng1.unidentified.str.contains(substring), "match"] = False
    fuzzyCosIng1.loc[(fuzzyCosIng1.identified == identified) & ~fuzzyCosIng1.unidentified.str.contains(substring), "match"] = True

unidentifiedExcludeDict = {"ALOE BARBADENSIS LEAF WATER (AND) CITRUS LIMON (LEMON) PEEL WATER (AND) MORINGA OLEIFERA LEAF WATER (AND) GLUCONOLACTONE (AND) SODIUM BENZOATE": r"^(WATER|ALOE BARBADENSIS LEAF)$",
                           "BALSAM": r"ACETYLATED|FLOWER|LEAF|IMPATIENS",
                           "BANANA": "FERMENT", "BIRREA SEED OIL": "ESTERS",
                           "BUTYLENE/ETHYLENE/STYRENE": "HYDROGENATED",
                           "CACAO SEED BUTTER": "HYDROLYZED",
                           "CARAPA GUAIANENSIS": "ESTERS",
                           "GLYCERIN & WATER & GYNOSTEMMA PENTAPHYLLUM EXTRACT & PANAX GINSENG ROOT EXTRACT": r"^PANAX GINSENG ROOT$",
                           "PORTULACA OLERACEA": "FERMENT"
                           }
for unidentified, substring in identifiedExcludeDict.items():
    fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified == unidentified) & fuzzyCosIng1.identified.str.contains(substring), "match"] = False
    fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified == unidentified) & ~fuzzyCosIng1.identified.str.contains(substring), "match"] = True

fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified.str.contains(r"^CAMELLIA OLEIFERA( LEAF EXTRACT)?") & (fuzzyCosIng1.identified == "CAMELLIA OLEIFERA LEAF EXTRACT")), "match"] = True
fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified.str.contains(r"^CAMELLIA OLEIFERA( LEAF EXTRACT)?") & (fuzzyCosIng1.identified != "CAMELLIA OLEIFERA LEAF EXTRACT")), "match"] = False
fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified.str.contains(r"^CAMELLIA OLEIFERA SEED OIL") & (fuzzyCosIng1.identified == "CAMELLIA OLEIFERA SEED OIL")), "match"] = True
fuzzyCosIng1.loc[fuzzyCosIng1.unidentified.str.contains("CAPRYLOYL GLYCERIN/SEBACIC ACID COPOLYMER") & fuzzyCosIng1.unidentified.str.contains("LAURETH-23") & fuzzyCosIng1.identified.isin(["CAPRYLOYL GLYCERIN/SEBACIC ACID COPOLYMER", "LAURETH-23"]), "match"] = True
fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified.str.contains("CAPRYLOYL GLYCERIN/SEBACIC ACID COPOLYMER") & fuzzyCosIng1.unidentified.str.contains("LAURETH-23")) & ~fuzzyCosIng1.identified.isin(["CAPRYLOYL GLYCERIN/SEBACIC ACID COPOLYMER", "LAURETH-23"]), "match"] = False
fuzzyCosIng1.loc[(fuzzyCosIng1.identified == "CLAY") & ~fuzzyCosIng1.unidentified.str.contains("MOROCC|FULLER"), "match"] = True
fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified == "COCAMIDOPROPYL BETAINE VP/VA COPOLYMER POLYSORBATE 20 GLYCERIN SODIUM COCOYL GLUTAMATE LINIUM USITATISSIMUM (LINSEED) SEED EXTRACT HYDROLYZED KERATIN HYDROLYZED QUINOA HYDROLYZED WHEAT PROTEIN HYDROLYZED WHEAT STARCH HYDROXYPROPYLTRIMONIUM HONEY") & fuzzyCosIng1.identified.isin(["KERATIN", "HONEY"]), "match"] = False
fuzzyCosIng1.loc[(fuzzyCosIng1.unidentified == "COCAMIDOPROPYL BETAINE VP/VA COPOLYMER POLYSORBATE 20 GLYCERIN SODIUM COCOYL GLUTAMATE LINIUM USITATISSIMUM (LINSEED) SEED EXTRACT HYDROLYZED KERATIN HYDROLYZED QUINOA HYDROLYZED WHEAT PROTEIN HYDROLYZED WHEAT STARCH HYDROXYPROPYLTRIMONIUM HONEY") & ~fuzzyCosIng1.identified.isin(["KERATIN", "HONEY"]), "match"] = True

identifiedIncludeWordBoundary = ["SAND", "SILICA", "CAMELLIA KISSI SEED OIL",
                                 "CITRUS AURANTIUM AMARA FLOWER WATER",
                                 "ETHYLHEXYLGLYCERIN"]
for identified in identifiedIncludeWordBoundary:
    regex = r"\b" + identified + r"\b"
    fuzzyCosIng1.loc[(fuzzyCosIng1.identified == identified) & fuzzyCosIng1.unidentified.str.contains(regex), "match"] = True
    fuzzyCosIng1.loc[(fuzzyCosIng1.identified == identified) & ~fuzzyCosIng1.unidentified.str.contains(regex), "match"] = False


"""Let's examine the unidentified ingredients that are only matched to 1
identified ingredient each."""
fuzzyCosIng1_1to1 = (fuzzyCosIng1.query("identifiedMatches == 1")
                     .sort_values(["identified"], ignore_index=True)
                     )
fuzzyCosIng1_multiple = (fuzzyCosIng1.query("identifiedMatches > 1")
                         .sort_values(["unidentified"], ignore_index=True)
                         )
identifiedRight_1to1 = ["ACRYLATES COPOLYMER", "AQUA", "MORINGA OLEIFERA SEED OIL",
                        "ETHYLHEXYL HYDROXYSTEARATE", "PERSEA GRATISSIMA OIL",
                        "OCTYLACRYLAMIDE/ACRYLATES/BUTYLAMINOETHYL METHACRYLATE COPOLYMER",
                        "POLLEN", "SEA WATER", "SILICA DIMETHYL SILYLATE",
                        "SIMMONDSIA CHINENSIS SEED OIL", "SYNTHETIC WAX",
                        "ULMUS FULVA BARK", "URTICA DIOICA", "GLYCERIN",
                        "VINYL DIMETHICONE/METHICONE SILSESQUIOXANE CROSSPOLYMER",
                        "VP/DMAPA ACRYLATES COPOLYMER", "ZEA MAYS GERM OIL",
                        "ADIPIC ACID/NEOPENTYL GLYCOL CROSSPOLYMER",
                        "CAPSICUM ANNUUM FRUIT EXTRACT", "LIMONENE",
                        "ELAEIS GUINEENSIS OIL", "LIMNANTHES ALBA SEED OIL",
                        "GLYCYRRHIZA GLABRA ROOT EXTRACT",
                        "MALVA SYLVESTRIS EXTRACT",
                        "MORINDA CITRIFOLIA EXTRACT"]
identifiedRightRegex_1to1 = r"^(ADANSONIA|AGAVE|ANTHEMIS NOBILIS|ARCTIUM LAPPA|ARNICA|ARTEMISIA|ASCOPHYLLUM|ASPALATHUS|BACOPA|BAMBUSA|BETULA|BRASSICA|CAESALPINIA|CALCIUM|CALLUNA|CALOPHYLLUM|CAPRYLIC/CAPRIC/[A-Z]{6}IC|CAPRYLO?YL|CA[RUV]|CENTAUREA|CETEARYL|CHAMOMILLA|CHENOPODIUM|CHONDRUS|CI [0-9]{5}(?!\d)|CITRUS [ABLPR]|COCO|CO[LMPR]|CRAMBE|CROTON|CUCURBITA|CURCUMA|DI([AC]|METHICONE|PALMITOYL|POTASSIUM|PTEROCARPUS|SODIUM)|DYSOXYLUM|ECHI|E[QR]|EU(GENIA|PHORBIA|TERPE)|GA|H(D|E[DN]|I)|HYDRO(LYZED|XYISOHEXYL)|IPDI|[JKQ]|LAVANDULA|LE|MACRO|MAGNESIUM|MAHONIA|MALUS|MALVIA|MANGI|MATRICARIA|MUSA|MYRCIARIA|OLEA|OPUNTIA|ORIGANUM|PANAX GINSENG ROOT|PENTAERYTHRITYL|PHY|PINE|PLU|POGO|POLYGLYCERYL|POTASSIUM|PRUNUS|PSIDIUM|PY|R[HI]|ROSA (CANINA|CENTIFOLIA|DAMASCENA)|RUBUS|SAL|SCLEROCARYA|SODIUM [BCHP]|SPIR|T.(?!COPHEROL)|VANILLA|VICTORIA|VINCA)"
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.identified.isin(identifiedRight_1to1) | fuzzyCosIng1_1to1.identified.str.contains(identifiedRightRegex_1to1), "match"] = True
identifiedWrong_1to1 = ["ETHYLENEDIAMINE", "ORYZA SATIVA", "CITRUS GRANDIS",
                        "APPLE CIDER VINEGAR", "CAPRYLIC/CAPRIC GLYCERIDES",
                        "CITRUS NOBILIS", "TOCOPHEROL"
                        ]
identifiedWrongRegex_1to1 = r"^(ASPERGILLUS|BACILLUS|DIMETHICONOL|ETHYL [EO]|HE[PX]|ISOPROPYL|ROS. EXTRACT|SACCHAROMYCES.|SODIUM [LRT])"
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.identified.isin(identifiedWrong_1to1) | fuzzyCosIng1_1to1.identified.str.contains(identifiedWrongRegex_1to1), "match"] = False
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.unidentified.str.contains("77499") & fuzzyCosIng1_1to1.unidentified.str.contains("77492"), "multipleIngredients"] = True
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.identified.str.contains(r"^HYDROGENATED") & (fuzzyCosIng1_1to1.unidentified != "BEHENYL ESTERS"), "match"] = False
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.identified.str.contains(r"^LACTOBACILLUS") & ~fuzzyCosIng1_1to1.unidentified.str.contains("FERMENT"), "match"] = False
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.identified.str.contains(r"^LACTOBACILLUS") & fuzzyCosIng1_1to1.unidentified.str.contains("FERMENT"), "match"] = True
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.identified.str.contains(r"^PEG-") & fuzzyCosIng1_1to1.identified.str.contains(r"^PEG-(14M|175|35)"), "match"] = True
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.identified.str.contains(r"^PEG-") & ~fuzzyCosIng1_1to1.identified.str.contains(r"^PEG-(14M|175|35)"), "match"] = False
fuzzyCosIng1_1to1.loc[(fuzzyCosIng1_1to1.identified == "WATER") & fuzzyCosIng1_1to1.unidentified.str.contains(r"(^WATER)|ONIZED WATER"), "match"] = True
fuzzyCosIng1_1to1.loc[(fuzzyCosIng1_1to1.identified == "WATER") & fuzzyCosIng1_1to1.match.isna(), "match"] = False
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.identified.str.contains("FERMENT") & ~fuzzyCosIng1_1to1.unidentified.str.contains("FERMENT"), "match"] = False

identifiedExcludeDict_1to1 = {"GOLD": r"JOJOBA|SEAL",
                              "PVP": r"POLYMER|HYDROTRITICUM",
                              "SACCHAROMYCES": r"FERMENT|EXTRACT",
                              "SILK": r"OAT|QUATERNIUM|BAMBOO|GUAR"}
for identified, substring in identifiedExcludeDict_1to1.items():
    fuzzyCosIng1_1to1.loc[(fuzzyCosIng1_1to1.identified == identified) & fuzzyCosIng1_1to1.unidentified.str.contains(substring), "match"] = False
    fuzzyCosIng1_1to1.loc[(fuzzyCosIng1_1to1.identified == identified) & ~fuzzyCosIng1_1to1.unidentified.str.contains(substring), "match"] = True

fuzzyCosIng1_1to1.loc[(fuzzyCosIng1_1to1.identified == "COTTON") & fuzzyCosIng1_1to1.identified.str.contains(r"\bCOTTON\b") & ~fuzzyCosIng1_1to1.unidentified.str.contains("THISTLE"), "match"] = True
fuzzyCosIng1_1to1.loc[(fuzzyCosIng1_1to1.identified == "COTTON") & (~fuzzyCosIng1_1to1.identified.str.contains(r"\bCOTTON\b") | fuzzyCosIng1_1to1.unidentified.str.contains("THISTLE")), "match"] = False
remainingFalse_1to1 = ["CLAY", "COUMARIN", "CROSCARMELLOSE"]
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.identified.isin(remainingFalse_1to1) & fuzzyCosIng1_1to1.match.isna(), "match"] = False
fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.match.isna(), "match"] = False
"""Wish that I could have redone fuzzy string matching with
partial_token_set_ratio(), but gotta move on now 'cause I don't have time"""

fuzzyCosIng1multiple_classified = fuzzyCosIng1_multiple.query("match.notna()")
fuzzyCosIng1_identified = fuzzyCosIng1_1to1.loc[fuzzyCosIng1_1to1.match == True, "unidentified"].drop_duplicates().tolist() + fuzzyCosIng1_multiple.loc[fuzzyCosIng1_multiple.match == True, "unidentified"].drop_duplicates().tolist()
fuzzyCosIng1multiple_unclassified = (fuzzyCosIng1_multiple.query("match.isna()")
                                     .query("unidentified != @fuzzyCosIng1_identified")
                                     )
"""I've already classified some ingredient names that are matched to multiple
INCI names. Some of the things I did in my classification was see which
ingredient names genuinely include multiple ingredient names. Now, I'm going to
assume that the remaining unclassified ingredient names are ingredient names
that refer only to single ingredients. For each of these names, I shall filter
down the combination(s) of unidentified ingredient names and INCI names that
most closely match in length."""


def lengthsFilter(df):
    """
    To be used in a DataFrame.groupby().apply() in which the dataframe is
    fuzzyCosIng1multiple_unclassified and is grouped by unidentified ingredient
    names. Filters the dataframe down to the combination of unidentified
    ingredient name and INCI name that most closely match in length

    Parameters
    ----------
    df : Grouped Pandas dataframe
        Pandas dataframe grouped by 'unidentified'.

    Returns
    -------
    The same dataframe with an additional column describing which combination
    to filter for.

    """
    maxLengthRatio = df.lengthsRatio.max()
    df = df.loc[df.lengthsRatio == maxLengthRatio]
    return df


fuzzyCosIng1multiple_unclassified2 = (fuzzyCosIng1multiple_unclassified.groupby("unidentified").apply(lengthsFilter)
                                      .reset_index(drop=True)
                                      .sort_values(["lengthsRatio", "identified"], ascending=[False, True], ignore_index=True)
                                      )
unidentifiedWrongMultiple_unclassified = ["SUNFLOWER GLYCERIDES", "ETHYLHEXYL",
                                          "PPG-5 CETETH-10 PHOSPHATE", "CUMIN",
                                          "SEBACIC ACID COPOLYMER", "ZEA MAYS",
                                          "PROPYLENE/STYRENE COPOLYMER",
                                          "OCTYLDODECYL", "TETRAISOSTEARATE",
                                          "STYRENEACRYLATES COPOLYMER",
                                          "DIETHONIUM SUCCINOYL HYDROLYZED PEA PROTEIN",
                                          "SODIUM ACRYLOYLDIMETHYL TAURATE COPOLYMER",
                                          "PEG-40 HYDROGENATED", "PHYTOSTERYL",
                                          "MEADOWFOAM SEED OIL", "VIOLET 2",
                                          "HYGROGENATED STYRENE/ISOPRENE COPOLYMER",
                                          "STYRENE / ACRYLATES COPOLYMER",
                                          "ISONONANOATE", "POLYGLYCERYL-4",
                                          "PENTAERYTHRITYL", "MEADOWFOAMATE",
                                          "DISODIUM LAURETH", "VP COPOLYMER",
                                          "DIMETHICONE/METHICONE COPOLYMER",
                                          "PPG-5 CETETH-20", "MEADOWFOAM SEED",
                                          "HYDROLYZED WHEAT PROTEINPVP CROSSPOLYMER",
                                          "PEG-7 DIMETHICONE ISOSTEARATE",
                                          "RASPBERRY SEED OIL",
                                          "DICAPRYLATE/DICAPRATE", "GREEN 5",
                                          "LONICERA JAPONICA",
                                          "COFFEE SEED OIL",
                                          ]
fuzzyCosIng1multiple_unclassified2.loc[fuzzyCosIng1multiple_unclassified2.unidentified.isin(unidentifiedWrongMultiple_unclassified), "match"] = False
fuzzyCosIng1multiple_unclassified2.loc[(fuzzyCosIng1multiple_unclassified2.identified == "WATER") & fuzzyCosIng1multiple_unclassified2.unidentified.str.contains(r"^WATER & "), "match"] = True
fuzzyCosIng1multiple_unclassified2.loc[(fuzzyCosIng1multiple_unclassified2.identified == "WATER") & ~fuzzyCosIng1multiple_unclassified2.unidentified.str.contains(r"^WATER & "), "match"] = False
fuzzyCosIng1multiple_unclassified2.loc[fuzzyCosIng1multiple_unclassified2.match.isna(), "match"] = True
# %%
"""Ok. No more fuzzy string matching. I'm not gonna repeat with fuzzy string
matching for CosIng. I'm just done.

I'm going to put together an Excel file containing just the fuzzy string
matching results with CosIng as the output for this script. Then in another
script, I will compile all of the strict and fuzzy string matching results
together. The Excel file for this script will contain the following tabs
- a table of unidentified ingredient names, INCI names, and CAS RNs if
available for INCI names
- a list of CAS RNs for INCI names that have CAS RNs
- a list of remaining unidentified ingredients
- a ReadMe
"""
fuzzyCosIngIdentified = (pd.concat([fuzzyCosIng1_1to1, fuzzyCosIng1multiple_classified, fuzzyCosIng1multiple_unclassified2], ignore_index=True)
                         .filter(["unidentified", "identified", "match"])
                         .query("match == True")
                         .drop_duplicates()
                         .reset_index(drop=True)
                         .merge(CosIng, "left", left_on="identified", right_on="INCI")
                         .filter(["unidentified", "INCI", "CASRN"])
                         .rename(columns={"CASRN": "CosIngCASRN"})
                         )
CASRN = (fuzzyCosIngIdentified.filter(["CosIngCASRN"])
         .drop_duplicates()
         .query("CosIngCASRN != '-'")
         )
unidentifiedAfterCosIng = (unidentifiedAfterCompTox.merge(fuzzyCosIngIdentified, "left", left_on="allCaps", right_on="unidentified")
                           .query("INCI.isna()")
                           .filter(["ingredientName", "allCaps"])
                           .drop_duplicates()
                           )
note = ["This file contains the results from my fuzzy string matching between",
        "unidentified ingredient names and INCI names from CosIng. This is",
        "after (1) strict matching with CompTox via the CompTox batch search,",
        "(2) strict matching with CosIng, and (3) fuzzy string matching with",
        "the results of the CompTox batch search. I will stop fuzzy string",
        "matching here. This file does not contain the results from these 3",
        "prior steps. I will create another Excel file later containing all",
        "the ingredient identification results later.",
        "",
        "The tab 'Fuzzy CosIng' contains the fuzzy string matching results.",
        "The column 'unidentified' are the original ingredient names in which",
        "all letters are in uppercase. These names are matched to INCI names",
        "(INCI), some of which have CAS RNs (CosIngCASRN). I also listed",
        "these CAS RNs in another tab called 'CosIng CASRNs' to facilitate",
        "doing a batch search of these CAS RNs on CompTox later. Any",
        "remaining unidentified ingredients are in the tab called 'Unidentified'."]
readMe = pd.DataFrame({"Note": note})
exportPath = outputFolder/"CosIng fuzzy.xlsx"
if os.path.exists(exportPath) is False:
    with pd.ExcelWriter(exportPath) as w:
        readMe.to_excel(w, "ReadMe", index=False)
        fuzzyCosIngIdentified.to_excel(w, "Fuzzy CosIng", index=False)
        CASRN.to_excel(w, "CosIng CASRNs", index=False)
        unidentifiedAfterCosIng.to_excel(w, "Unidentified", index=False)
