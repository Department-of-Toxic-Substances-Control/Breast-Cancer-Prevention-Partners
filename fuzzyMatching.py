# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 13:02:10 2026

@author: BChung

I've identified some ingredient names by performing case-insensitive strict
matching first with CompTox and then second with CosIng. Ingredients that I
couldn't identified using CompTox were then identified with CosIng. This still
leaves quite a lot of ingredient names that aren't identified. This script
performs fuzzy string matching between ingredients that are still unidentified
and ingredients that were identified using CompTox and all ingredients on
CosIng.
"""
import os
from pathlib import Path
import pandas as pd
from rapidfuzz import fuzz

repository = Path(os.getcwd())
repositoryFolder = Path(os.path.dirname(repository))
dataFolder = repositoryFolder/"Data"
inputFolder = dataFolder/"Input"
outputFolder = dataFolder/"Output"

identifiedUnidentifiedPath = outputFolder/"Identified & unidentified after batch search.xlsx"
identifiedCompTox = pd.read_excel(identifiedUnidentifiedPath, "Identified CompTox", dtype="string")
identifiedCosIng = (pd.read_excel(identifiedUnidentifiedPath, "Identified CosIng", dtype="string")
                    .rename(columns={"CASRN": "CosIngCASRN"})
                    )
unidentifiedOG = pd.read_excel(identifiedUnidentifiedPath, "Unidentified", dtype="string")

CosIngPath = inputFolder/"Cleaned CosIng database - scraped on January 21, 2026.xlsx"
CosIngDF = (pd.read_excel(CosIngPath, "Substances", usecols=[1, 3])
            .drop_duplicates()
            )
# %%
"""Now to do fuzzy string matching. I'm going to do fuzzy string matching
between the unidentified ingredients and the ingredient names of the
ingredients identified with CompTox first, and then in another chunk I will
perform fuzzy string matching between ingredients that still aren't identified
and all CosIng INCI names"""
unidentifiedDF = unidentifiedOG.copy()
unidentifiedDF["allCaps"] = unidentifiedDF.ingredientName.str.upper()
identifiedCompTox["allCaps"] = identifiedCompTox.ingredientName.str.upper()


def fuzzy(unidentifiedList, identifiedList, similarityFunction, similarityName, cutoff, cutoffType, wrongMatches=None):
    """
    Creates combinations of 2 elements each, 1 element from a list of
    unidentified ingredient names and 1 element from a list of identified
    ingredient names, and then compare the 2 elements in each combination to
    see if they are similar. This function first compares the length of each
    string in a combination, then will see how similar each combination is
    using a scoring function from rapidfuzz. The output is a dataframe with
    each combination, a ratio of the strings' lengths, and the combiantion's
    similarity score. The ratio of string lengths is shortest : longest.

    Parameters
    ----------
    unidentifiedList : list
        A list of unidentified ingredient names.
    identifiedList : str
        A list of identified ingredient names.
    similarityFunction : fuzz function
        A function that scores similarities between 2 strings imported from
        rapidfuzz.fuzz.
    similarityName : str
        The name of the similarity score. The wording should be similar to rapidfuzz.fuzz
    cutoff : numeric
        A minimum, numeric cutoff that a combination must exceed for the
        combination to be kept. The type of cutoff is to be specified elsewhere
    cutoffType : str
        The type of cutoff. Valid values are either 'length', 'similarity', or
        'product', with 'product' being 'length' multiplied by 'similarity' and
        weighing both equally
    wrongMatches : Pandas dataframe, default is None
        A dataframe with columns 'unidentified' & 'identified' where each row
        is a combination of an unidentified name and an identified name that
        is not a correct match. This dataframe will be used to narrow down the
        possible combinations in the output dataframe.

    Returns
    -------
    A dataframe with each row representing a combination, a ratio of their
    lengths, and a similarity score.

    """
    combinations = []
    if cutoffType == "length":
        for unidentified in unidentifiedList:
            for identified in identifiedList:
                lengths = [len(unidentified), len(identified)]
                ratio = min(lengths)/max(lengths)
                score = similarityFunction(unidentified, identified)
                if ratio >= cutoff:
                    combinations.append([unidentified, identified, ratio, score])
    elif cutoffType == "similarity":
        for unidentified in unidentifiedList:
            for identified in identifiedList:
                lengths = [len(unidentified), len(identified)]
                ratio = min(lengths)/max(lengths)
                score = similarityFunction(unidentified, identified)
                if score >= cutoff:
                    combinations.append([unidentified, identified, ratio, score])
    elif cutoffType == "product":
        for unidentified in unidentifiedList:
            for identified in identifiedList:
                lengths = [len(unidentified), len(identified)]
                ratio = min(lengths)/max(lengths)
                score = similarityFunction(unidentified, identified)
                product = ratio*score
                if product >= cutoff:
                    combinations.append([unidentified, identified, ratio, score])
    combinationsDF = pd.DataFrame(combinations, columns=["unidentified", "identified", "lengthsRatio", similarityName])
    if wrongMatches is not None:
        print("Wrong matches inputted")
        combinationsDF = (combinationsDF.merge(wrongMatches, "left", ["unidentified", "identified"], indicator=True)
                          .query("_merge == 'left_only'")
                          .drop(columns=["_merge"])
                          )
        print(combinationsDF.shape)
    combinationsDF = combinationsDF.sort_values([similarityName, "lengthsRatio", "unidentified"], ascending=[False, False, True], ignore_index=True)
    return combinationsDF


unidentifiedAfterStrict = unidentifiedDF.allCaps.drop_duplicates().tolist()
identifiedCompToxList = identifiedCompTox.allCaps.drop_duplicates().tolist()
fuzzyCompTox1 = fuzzy(unidentifiedAfterStrict, identifiedCompToxList, fuzz.partial_ratio, "partialRatio", 85, "product")

"""Start assigning which combinations are true matches and which ones aren't"""
fuzzyCompTox1.loc[:8, "match"] = True
fuzzyCompTox1.loc[9:10, "match"] = False
fuzzyCompTox1.loc[11:37, "match"] = True
fuzzyCompTox1.loc[45:83, "match"] = True
fuzzyCompTox1.loc[88:94, "match"] = True
fuzzyCompTox1.loc[95:99, "match"] = False
fuzzyCompTox1.loc[100:102, "match"] = True
fuzzyCompTox1.loc[103:105, "match"] = False
fuzzyCompTox1.loc[106:108, "match"] = True
fuzzyCompTox1.loc[110:123, "match"] = True
fuzzyCompTox1.loc[125:183, "match"] = True
fuzzyCompTox1.loc[184:196, "match"] = False
fuzzyCompTox1.loc[197:223, "match"] = True
fuzzyCompTox1.loc[224:225, "match"] = False
fuzzyCompTox1.loc[231:233, "match"] = False
fuzzyCompTox1.loc[234:250, "match"] = True
fuzzyCompTox1.loc[251:252, "match"] = False
fuzzyCompTox1.loc[253:310, "match"] = True
fuzzyCompTox1.loc[312:330, "match"] = True
fuzzyCompTox1.loc[332:335, "match"] = True
fuzzyCompTox1.loc[337:366, "match"] = True
fuzzyCompTox1.loc[367:371, "match"] = False
fuzzyCompTox1.loc[372:378, "match"] = True
fuzzyCompTox1.loc[379:381, "match"] = False
fuzzyCompTox1.loc[382:396, "match"] = True
fuzzyCompTox1.loc[398:412, "match"] = True
fuzzyCompTox1.loc[414:428, "match"] = True
fuzzyCompTox1.loc[430:432, "match"] = True
fuzzyCompTox1.loc[433:436, "match"] = False
fuzzyCompTox1.loc[438:440, "match"] = False
fuzzyCompTox1.loc[443:456, "match"] = True
fuzzyCompTox1.loc[458:460, "match"] = True
fuzzyCompTox1.loc[462:463, "match"] = True
fuzzyCompTox1.loc[465:469, "match"] = True
fuzzyCompTox1.loc[471:485, "match"] = True
fuzzyCompTox1.loc[486:487, "match"] = False
fuzzyCompTox1.loc[488:489, "match"] = True
fuzzyCompTox1.loc[490:493, "match"] = False
fuzzyCompTox1.loc[494:513, "match"] = True
fuzzyCompTox1.loc[515:537, "match"] = True
fuzzyCompTox1.loc[538:542, "match"] = False
fuzzyCompTox1.loc[543:548, "match"] = True
fuzzyCompTox1.loc[550:572, "match"] = True
fuzzyCompTox1.loc[573:575, "match"] = False
fuzzyCompTox1.loc[577:579, "match"] = False
fuzzyCompTox1.loc[581:583, "match"] = False
fuzzyCompTox1.loc[584:585, "match"] = True
fuzzyCompTox1.loc[586:594, "match"] = False
fuzzyCompTox1.loc[597:614, "match"] = True
fuzzyCompTox1.loc[615:668, "match"] = False
fuzzyCompTox1.loc[669:676, "match"] = True
fuzzyCompTox1.loc[677:691, "match"] = False
fuzzyCompTox1.loc[692:704, "match"] = True
fuzzyCompTox1.loc[709:715, "match"] = False
fuzzyCompTox1.loc[717:719, "match"] = False
fuzzyCompTox1.loc[720:721, "match"] = True
fuzzyCompTox1.loc[725:727, "match"] = False
fuzzyCompTox1.loc[728:729, "match"] = True
fuzzyCompTox1.loc[731:733, "match"] = True
fuzzyCompTox1.loc[735:738, "match"] = True
fuzzyCompTox1.loc[739:740, "match"] = False
fuzzyCompTox1.loc[743:744, "match"] = False
fuzzyCompTox1.loc[745:746, "match"] = True
fuzzyCompTox1.loc[748:759, "match"] = True
fuzzyCompTox1.loc[760:775, "match"] = False
fuzzyCompTox1.loc[776:777, "match"] = True
fuzzyCompTox1.loc[778:785, "match"] = False
fuzzyCompTox1.loc[786:793, "match"] = True
fuzzyCompTox1.loc[795:797, "match"] = True
fuzzyCompTox1.loc[799:800, "match"] = True
fuzzyCompTox1.loc[801:803, "match"] = False
fuzzyCompTox1.loc[806:821, "match"] = True
fuzzyCompTox1.loc[823:832, "match"] = True
fuzzyCompTox1.loc[833:836, "match"] = False
fuzzyCompTox1.loc[837:857, "match"] = True
fuzzyCompTox1.loc[858:863, "match"] = False
fuzzyCompTox1.loc[864:891, "match"] = True
fuzzyCompTox1.loc[892:895, "match"] = False
fuzzyCompTox1.loc[896:905, "match"] = True
fuzzyCompTox1.loc[909:914, "match"] = True
fuzzyCompTox1.loc[915:916, "match"] = False
fuzzyCompTox1.loc[917:922, "match"] = True
fuzzyCompTox1.loc[925:926, "match"] = False
fuzzyCompTox1.loc[927:930, "match"] = True
fuzzyCompTox1.loc[931:935, "match"] = False
fuzzyCompTox1.loc[936:938, "match"] = True
fuzzyCompTox1.loc[939:940, "match"] = False
fuzzyCompTox1.loc[943:948, "match"] = True
fuzzyCompTox1.loc[950:965, "match"] = True
fuzzyCompTox1.loc[967:969, "match"] = True
fuzzyCompTox1.loc[970:972, "match"] = False
fuzzyCompTox1.loc[974:975, "match"] = False
fuzzyCompTox1.loc[976:996, "match"] = True
fuzzyCompTox1.loc[999:1000, "match"] = False
fuzzyCompTox1.loc[1001:1003, "match"] = True
fuzzyCompTox1.loc[1006:1007, "match"] = False
fuzzyCompTox1.loc[1008:1009, "match"] = True
fuzzyCompTox1.loc[1010:1011, "match"] = False
fuzzyCompTox1.loc[1012:1013, "match"] = True
fuzzyCompTox1.loc[1016:1017, "match"] = False
fuzzyCompTox1.loc[1018:1019, "match"] = True
fuzzyCompTox1.loc[1020:1038, "match"] = False
fuzzyCompTox1.loc[1039:1040, "match"] = True
fuzzyCompTox1.loc[1041:1043, "match"] = False
fuzzyCompTox1.loc[1044:1045, "match"] = True
fuzzyCompTox1.loc[1046:1047, "match"] = False
fuzzyCompTox1.loc[1048:1066, "match"] = True
fuzzyCompTox1.loc[1067:1068, "match"] = False
fuzzyCompTox1.loc[1069:1071, "match"] = True
fuzzyCompTox1.loc[1072:1097, "match"] = False
fuzzyCompTox1.loc[1099:1100, "match"] = False
fuzzyCompTox1.loc[1101:1103, "match"] = True
fuzzyCompTox1.loc[1104:1165, "match"] = False
fuzzyCompTox1.loc[1166:1168, "match"] = True
fuzzyCompTox1.loc[1169:1175, "match"] = False
fuzzyCompTox1.loc[1176:1178, "match"] = True
fuzzyCompTox1.loc[1180:1210, "match"] = False
fuzzyCompTox1.loc[1212:1217, "match"] = False
fuzzyCompTox1.loc[1219:1278, "match"] = False
fuzzyCompTox1.loc[1279:1282, "match"] = True
fuzzyCompTox1.loc[1283:1285, "match"] = False
fuzzyCompTox1.loc[1286:1288, "match"] = True
fuzzyCompTox1.loc[1289:1294, "match"] = False
fuzzyCompTox1.loc[1295:1298, "match"] = True
fuzzyCompTox1.loc[1300:1306, "match"] = True
fuzzyCompTox1.loc[1309:1323, "match"] = False
fuzzyCompTox1.loc[1325:1327, "match"] = True
fuzzyCompTox1.loc[1328:1330, "match"] = False
fuzzyCompTox1.loc[1333:1334, "match"] = True
fuzzyCompTox1.loc[1335:1341, "match"] = False
fuzzyCompTox1.loc[1342:1343, "match"] = True
fuzzyCompTox1.loc[1344:1347, "match"] = False
fuzzyCompTox1.loc[1351:1358, "match"] = True
fuzzyCompTox1.loc[1359:1370, "match"] = False
fuzzyCompTox1.loc[1371:1373, "match"] = True
fuzzyCompTox1.loc[1374:1377, "match"] = False
fuzzyCompTox1.loc[1378:1379, "match"] = True
fuzzyCompTox1.loc[1380:1382, "match"] = False
fuzzyCompTox1.loc[1383:1385, "match"] = True
fuzzyCompTox1.loc[1387:1388, "match"] = True
fuzzyCompTox1.loc[1389:1391, "match"] = False
fuzzyCompTox1.loc[1394:1398, "match"] = True
fuzzyCompTox1.loc[1399:1404, "match"] = False
fuzzyCompTox1.loc[1406:1407, "match"] = False
fuzzyCompTox1.loc[1409:1410, "match"] = False
fuzzyCompTox1.loc[1412:1413, "match"] = False
fuzzyCompTox1.loc[1414:1417, "match"] = True
fuzzyCompTox1.loc[1419:1422, "match"] = True
fuzzyCompTox1.loc[1425:1428, "match"] = False
fuzzyCompTox1.loc[1429:1434, "match"] = True
fuzzyCompTox1.loc[1435:1445, "match"] = False
fuzzyCompTox1.loc[1447:1462, "match"] = False
fuzzyCompTox1.loc[1463:1464, "match"] = True
fuzzyCompTox1.loc[1465:1479, "match"] = False
fuzzyCompTox1.loc[1481:1487, "match"] = False
fuzzyCompTox1.loc[1489:1491, "match"] = False
fuzzyCompTox1.loc[1492:1493, "match"] = True
fuzzyCompTox1.loc[1496:1498, "match"] = False
fuzzyCompTox1.loc[1500:1522, "match"] = False
fuzzyCompTox1.loc[1523:1524, "match"] = True
fuzzyCompTox1.loc[1525:1527, "match"] = False
fuzzyCompTox1.loc[1528:1529, "match"] = True
fuzzyCompTox1.loc[1530:1537, "match"] = False
fuzzyCompTox1.loc[1539:1545, "match"] = False
fuzzyCompTox1.loc[1546:1548, "match"] = True
fuzzyCompTox1.loc[1549:1550, "match"] = False
fuzzyCompTox1.loc[1552:1555, "match"] = False
fuzzyCompTox1.loc[1557:1576, "match"] = False
fuzzyCompTox1.loc[1578:1591, "match"] = False
fuzzyCompTox1.loc[1592:1595, "match"] = True
fuzzyCompTox1.loc[1596:1597, "match"] = False
fuzzyCompTox1.loc[1599:1600, "match"] = False
fuzzyCompTox1.loc[1601:1602, "match"] = True
fuzzyCompTox1.loc[1603:1606, "match"] = False
fuzzyCompTox1.loc[1608:1623, "match"] = False
fuzzyCompTox1.loc[1625:1629, "match"] = False
fuzzyCompTox1.loc[1630:1631, "match"] = True
fuzzyCompTox1.loc[1632:1634, "match"] = False
fuzzyCompTox1wrong = [38, 44, 84, 87, 109, 124, 229, 311, 311, 336, 397, 413,
                      429, 442, 457, 461, 464, 470, 514, 540, 596, 705, 707,
                      724, 730, 734, 742, 747, 794, 798, 805, 822, 906, 908,
                      923, 942, 949, 966, 997, 1004, 1014, 1179, 1299, 1307,
                      1324, 1332, 1349, 1386, 1393, 1418, 1423, 1494]
fuzzyCompTox1right = [226, 228, 230, 437, 441, 576, 580, 595, 706, 708, 716,
                      723, 741, 804, 907, 924, 941, 973, 998, 1005, 1015, 1098,
                      1211, 1218, 1308, 1331, 1348, 1350, 1392, 1405, 1408,
                      1411, 1424, 1446, 1480, 1488, 1495, 1499, 1538, 1551,
                      1556, 1577, 1598, 1607, 1624, 1635]
fuzzyCompTox1.loc[fuzzyCompTox1wrong, "match"] = False
fuzzyCompTox1.loc[fuzzyCompTox1right, "match"] = True

unidentifiedDF = (unidentifiedDF.merge(fuzzyCompTox1, "left", left_on="allCaps", right_on="unidentified")
                  .query("identified.isna()")
                  .drop(columns=["unidentified", "identified", "partialRatio", "lengthsRatio", "match"])
                  )
fuzzyCompTox1identified = (fuzzyCompTox1.query("match == True")
                           .drop(columns=["lengthsRatio", "partialRatio", "match"])
                           )
fuzzyCompTox1wrong = (fuzzyCompTox1.query("match == False")
                      .drop(columns=["lengthsRatio", "partialRatio", "match"])
                      )
# %%
"""Going to repeat fuzzy string matching with ingredients that were identified
from case-insensitive strict matching with CompTox again."""

unidentifiedAfterFuzzy1 = unidentifiedDF.allCaps.drop_duplicates().tolist()
fuzzyCompTox2 = fuzzy(unidentifiedAfterFuzzy1, identifiedCompToxList, fuzz.token_set_ratio, "tokenSetRatio", 90, "similarity", fuzzyCompTox1wrong)

"""This produced > 9,300 combinations of possible matches, which is way too
much for me to manually inspect. Let's see, I'll just filter down to the
combinations with a lengths ratio of at least 1 and a similarity score of 100,
and then further filter down to the first 1500 rows
"""
fuzzyCompTox2 = fuzzyCompTox2.query("(lengthsRatio >= 0.1) & (tokenSetRatio == 100)")
fuzzyCompTox2 = fuzzyCompTox2.loc[:1500]
fuzzyCompTox2.loc[:44, "match"] = True
fuzzyCompTox2.loc[46:52, "match"] = True
fuzzyCompTox2.loc[54:94, "match"] = True
fuzzyCompTox2.loc[96:105, "match"] = True
fuzzyCompTox2.loc[107:116, "match"] = True
fuzzyCompTox2.loc[118:137, "match"] = True
fuzzyCompTox2.loc[139:198, "match"] = True
fuzzyCompTox2.loc[200:265, "match"] = True
fuzzyCompTox2.loc[267:380, "match"] = True
fuzzyCompTox2.loc[382:388, "match"] = True
fuzzyCompTox2.loc[390:468, "match"] = True
fuzzyCompTox2.loc[469:471, "match"] = False
fuzzyCompTox2.loc[472:484, "match"] = True
fuzzyCompTox2.loc[486:487, "match"] = True
fuzzyCompTox2.loc[489:491, "match"] = True
fuzzyCompTox2.loc[493:496, "match"] = True
fuzzyCompTox2.loc[498:520, "match"] = True
fuzzyCompTox2.loc[522:525, "match"] = True
fuzzyCompTox2.loc[527:551, "match"] = True
fuzzyCompTox2.loc[552:553, "match"] = False
fuzzyCompTox2.loc[554:560, "match"] = True
fuzzyCompTox2.loc[562:583, "match"] = True
fuzzyCompTox2.loc[585:587, "match"] = True
fuzzyCompTox2.loc[589:591, "match"] = True
fuzzyCompTox2.loc[595:600, "match"] = True
fuzzyCompTox2.loc[602:619, "match"] = True
fuzzyCompTox2.loc[621:626, "match"] = True
fuzzyCompTox2.loc[628:640, "match"] = True
fuzzyCompTox2.loc[642:649, "match"] = True
fuzzyCompTox2.loc[650:651, "match"] = False
fuzzyCompTox2.loc[652:662, "match"] = True
fuzzyCompTox2.loc[663:664, "match"] = False
fuzzyCompTox2.loc[665:672, "match"] = True
fuzzyCompTox2.loc[674:685, "match"] = True
fuzzyCompTox2.loc[689:708, "match"] = True
fuzzyCompTox2.loc[710:714, "match"] = True
fuzzyCompTox2.loc[717:718, "match"] = False
fuzzyCompTox2.loc[719:734, "match"] = True
fuzzyCompTox2.loc[736:739, "match"] = True
fuzzyCompTox2.loc[740:741, "match"] = False
fuzzyCompTox2.loc[742:749, "match"] = True
fuzzyCompTox2.loc[750:753, "match"] = False
fuzzyCompTox2.loc[755:756, "match"] = False
fuzzyCompTox2.loc[757:766, "match"] = True
fuzzyCompTox2.loc[767:768, "match"] = False
fuzzyCompTox2.loc[769:796, "match"] = True
fuzzyCompTox2.loc[800:802, "match"] = True
fuzzyCompTox2.loc[804:825, "match"] = True
fuzzyCompTox2.loc[826:827, "match"] = False
fuzzyCompTox2.loc[828:829, "match"] = True
fuzzyCompTox2.loc[832:833, "match"] = False
fuzzyCompTox2.loc[834:857, "match"] = True
fuzzyCompTox2.loc[859:862, "match"] = True
fuzzyCompTox2.loc[864:889, "match"] = True
fuzzyCompTox2.loc[891:892, "match"] = True
fuzzyCompTox2.loc[894:896, "match"] = True
fuzzyCompTox2.loc[897:899, "match"] = False
fuzzyCompTox2.loc[900:918, "match"] = True
fuzzyCompTox2.loc[919:920, "match"] = False
fuzzyCompTox2.loc[921:928, "match"] = True
fuzzyCompTox2.loc[930:931, "match"] = True
fuzzyCompTox2.loc[932:934, "match"] = False
fuzzyCompTox2.loc[935:963, "match"] = True
fuzzyCompTox2.loc[964:965, "match"] = False
fuzzyCompTox2.loc[966:969, "match"] = True
fuzzyCompTox2.loc[971:978, "match"] = True
fuzzyCompTox2.loc[980:984, "match"] = True
fuzzyCompTox2.loc[986:1002, "match"] = True
fuzzyCompTox2.loc[1004:1005, "match"] = True
fuzzyCompTox2.loc[1006:1007, "match"] = False
fuzzyCompTox2.loc[1012:1054, "match"] = True
fuzzyCompTox2.loc[1056:1058, "match"] = True
fuzzyCompTox2.loc[1061:1111, "match"] = True
fuzzyCompTox2.loc[1112:1116, "match"] = False
fuzzyCompTox2.loc[1117:1120, "match"] = True
fuzzyCompTox2.loc[1121:1122, "match"] = False
fuzzyCompTox2.loc[1123:1127, "match"] = True
fuzzyCompTox2.loc[1129:1162, "match"] = True
fuzzyCompTox2.loc[1166:1178, "match"] = True
fuzzyCompTox2.loc[1180:1181, "match"] = True
fuzzyCompTox2.loc[1183:1184, "match"] = True
fuzzyCompTox2.loc[1186:1196, "match"] = True
fuzzyCompTox2.loc[1198:1199, "match"] = True
fuzzyCompTox2.loc[1200:1202, "match"] = False
fuzzyCompTox2.loc[1204:1229, "match"] = True
fuzzyCompTox2.loc[1230:1231, "match"] = False
fuzzyCompTox2.loc[1232:1239, "match"] = True
fuzzyCompTox2.loc[1241:1249, "match"] = True
fuzzyCompTox2.loc[1251:1266, "match"] = True
fuzzyCompTox2.loc[1268:1292, "match"] = True
fuzzyCompTox2.loc[1294:1314, "match"] = True
fuzzyCompTox2.loc[1316:1320, "match"] = True
fuzzyCompTox2.loc[1323:1326, "match"] = False
fuzzyCompTox2.loc[1327:1329, "match"] = True
fuzzyCompTox2.loc[1331:1341, "match"] = True
fuzzyCompTox2.loc[1343:1359, "match"] = True
fuzzyCompTox2.loc[1361:1372, "match"] = True
fuzzyCompTox2.loc[1374:1383, "match"] = True
fuzzyCompTox2.loc[1385:1396, "match"] = True
fuzzyCompTox2.loc[1398:1416, "match"] = True
fuzzyCompTox2.loc[1418:1419, "match"] = True
fuzzyCompTox2.loc[1421:1422, "match"] = True
fuzzyCompTox2.loc[1424:1425, "match"] = True
fuzzyCompTox2.loc[1426:1427, "match"] = False
fuzzyCompTox2.loc[1428:1433, "match"] = True
fuzzyCompTox2.loc[1435:1443, "match"] = True
fuzzyCompTox2.loc[1445:1465, "match"] = True
fuzzyCompTox2.loc[1467:1478, "match"] = True
fuzzyCompTox2.loc[1479:1488, "match"] = False
fuzzyCompTox2.loc[1489:1497, "match"] = True
fuzzyCompTox2.loc[1499:, "match"] = True
fuzzyCompTox2wrong = [45, 53, 95, 106, 117, 138, 199, 266, 381, 389, 485, 488,
                      490, 492, 497, 521, 526, 561, 584, 588, 592, 594, 601,
                      620, 627, 641, 673, 686, 688, 709, 715, 735, 797, 799,
                      803, 830, 858, 863, 890, 893, 919, 929, 970, 979, 985,
                      1003, 1009, 1011, 1055, 1060, 1128, 1163, 1165, 1182,
                      1185, 1197, 1203, 1240, 1250, 1267, 1293, 1315, 1321,
                      1330, 1342, 1360, 1373, 1384, 1397, 1417, 1420, 1423,
                      1434, 1444, 1466, 1498]
fuzzyCompTox2right = [593, 687, 716, 754, 798, 831, 1008, 1010, 1059, 1164,
                      1322]
fuzzyCompTox2multiple = [181, 203, 316, 340, 364, 382, 436, 438, 508, 554,
                         580, 609, 632, 638, 645, 648, 661, 721, 748, 771, 776,
                         777, 798, 809, 862, 872, 921, 922, 926, 928, 975, 977,
                         978, 986, 991, 994, 995, 1013, 1043, 1048, 1050, 1051,
                         1053, 1056, 1058, 1066, 1088, 1090, 1094, 1095, 1096,
                         1118, 1134, 1143, 1153, 1155, 1168, 1173, 1177, 1179,
                         1196, 1208, 1229, 1237, 1239, 1266, 1277, 1282, 1286,
                         1287, 1289, 1291, 1292, 1357, 1362, 1364, 1414, 1418,
                         1433, 1447, 1465, 1473, 1474, 1475, 1478, 1494, 1496]
fuzzyCompTox2.loc[fuzzyCompTox2wrong, "match"] = False
fuzzyCompTox2.loc[fuzzyCompTox2right, "match"] = True
fuzzyCompTox2.loc[fuzzyCompTox2multiple, "multipleIngredients"] = True
fuzzyCompTox2.loc[fuzzyCompTox2.multipleIngredients.isna(), "multipleIngredients"] = False

fuzzyCompTox2wrong = (fuzzyCompTox2.query("match == False")
                      .filter(["unidentified", "identified"])
                      )
fuzzyWrong = pd.concat([fuzzyCompTox1wrong, fuzzyCompTox2wrong],
                       ignore_index=True)
fuzzyCompTox2identified_single = fuzzyCompTox2.query("(match == True) & (multipleIngredients == False)")
fuzzyCompTox2multiple = fuzzyCompTox2.query("multipleIngredients == True")
fuzzyCompTox2multiple_match = fuzzyCompTox2multiple.loc[fuzzyCompTox2multiple.unidentified.duplicated(False)]
fuzzyCompTox2multiple_unmatch = fuzzyCompTox2multiple.loc[~fuzzyCompTox2multiple.unidentified.duplicated(False)]
fuzzyCompTox2identified = (pd.concat([fuzzyCompTox2identified_single, fuzzyCompTox2multiple_match], ignore_index=True)
                           .filter(["unidentified", "identified"])
                           )
unidentifiedDF2 = (unidentifiedDF.merge(fuzzyCompTox2identified, "left", left_on="allCaps", right_on="unidentified")
                   .query("unidentified.isna()")
                   .filter(["ingredientName", "allCaps"])
                   )
# %%
unidentifiedAfterFuzzy2 = unidentifiedDF2.allCaps.drop_duplicates().tolist()
fuzzyCompTox3 = fuzzy(unidentifiedAfterFuzzy2, identifiedCompToxList, fuzz.partial_ratio, "partialRatio", 100, "similarity", fuzzyWrong)
fuzzyCompTox3["unidentifiedLength"] = fuzzyCompTox3.unidentified.str.len()
fuzzyCompTox3["identifiedLength"] = fuzzyCompTox3.identified.str.len()
fuzzyCompTox3 = fuzzyCompTox3.query("(unidentifiedLength > 3) & (identifiedLength > 2) & (lengthsRatio > 0.15)")
unidentifiedWrong = ["BLUE", "COCO", "CORN", "LAKE", "LEAF", "NANO", "OILS",
                     "CETYL", "EXTRACT", "HYDROGENATED", "HYDROXYETHYL",
                     "PALM", "ISONONANOATE", "ABSOLUTE", "FRUIT EXTRACT",
                     "OCTYLDODECYL", "EVENING", "OLEA", "PALMITOYL", "VINYL",
                     "MINT", "COPOLYMER", "FLOWER", "FLOWER OIL", "RICE",
                     "POLYMERS", "ACRYLONITRILE STYRENE COPOLYMER"]
unidentifiedRight = ["ISONONYL"]
identifiedWrong = ["ACETATE", "ACID", "ADIPATE", "ADIPIC ACID", "CAMPHOR",
                   "BENZYL", "BUTYLPHENYL", "CARBONATE", "CHAMOMILE", "BASED",
                   "CHLORIDE", "CHLOROPHYLL", "CINNAMAL", "BENZALDEHYDE",
                   "BENZOATE", "BENZOIN", "BUTYLENE", "BUTYRATE", "CARBON",
                   "2-ETHYLHEXYL PALMITATE", "ALFALFA", "ASCORBATE", "CLAYS",
                   "COCOYL SARCOSINE", "CYCLOHEXANE", "D LIMONENE", "DIMETHYL",
                   "DECYL OLEATE", "DEHYDROACETATE", "DENATONIUM BENZOATE",
                   "DENDRITIC SALT", "DI-C12-13 ALKYL TARTRATE", "DIACETATE",
                   "DIHYDROMYRICETIN", "DIMETHYLAMINE", "DIMETHYLAMINOETHANOL",
                   "DIPROPYLENE GLYCOL", "DISODIUM", "EAU", "EDTA", "GLYCERYL",
                   "DISODIUM CAPRYLOAMPHODIPROPIONATE", "EPSOM SALTS", "HEXYL",
                   "DISODIUM COCOYL GLUTAMATE", "ERYTHRITOL", "ETHYLCELLULOSE",
                   "FATTY ACIDS", "FATTY ALCOHOL", "FERROCYANIDE", "GLYCINE",
                   "FORMALDEHYDE", "GLYCOL", "HEXADECENE", "HEXYLENE", "LARD",
                   "HIBISCUS SABDARIFFA SEED OIL", "HYALURONATE", "HYDANTOIN",
                   "HIPPOPHAE RHAMNOIDES EXTRACT", "HYDRASTIS", "ISOBUTANE",
                   "HYDRATED SILICA", "HYDROXYETHYL ACRYLATE", "ISOPROPYL",
                   "HYDROXYPROPYL METHACRYLATE", "ISONONANOIC ACID", "LIME",
                   "HYDROXYPROPYLTRIMONIUM CHLORIDE", "ISONONYL ISONONANOATE",
                   "INDIGOFERA TINCTORIA EXTRACT", "KOJIC ACID", "LACTIC ACID",
                   "LACTYL LACTATE", "LAVENDER OIL", "MALLOW EXTRACT", "MEA",
                   "MALTOSE", "MANGANESE", "ALOE BARBADENSIS LEAF JUICE",
                   "MEL", "MENTHA SPICATA LEAF OIL", "METHICONE", "METHYL",
                   "MUSHROOM EXTRACT", "MUSK", "MYRICETIN", "MYRRH", "NIACIN",
                   "NEOPENTYL GLYCOL", "OIL OF PEPPERMINT", "OLETH-2", "OZONE",
                   "PARAFFIN", "PETROLEUM", "PHENYL", "PHOSPHATE", "PROPANOL",
                   "PHOSPHOLIPID", "PHTHALATES", "PHYTOSPHINGOSINE", "RUTIN",
                   "PHYTOSTERYL/OCTYLDODECYL LAUROYL GLUTAMATE", "PROPYLENE",
                   "POLYETHYLENE", "POLYGLYCERIN-10", "POLYSILOXANE", "SLS",
                   "POLYHYDROXYSTEARIC ACID", "PROPYLENE CARBONATE", "SULFATE",
                   "POLYMETHACRYLAMIDOPROPYLTRIMONIUM CHLORIDE", "SACCHARIN",
                   "PROPYLENE GLYCOL DIBENZOATE", "PROVITAMIN A", "RESORCINOL",
                   "PRUNUS ARMENIACA KERNEL OIL", "PRUNUS DULCIS OIL", "TIN",
                   "RAPESEED OIL", "RASPBERRYKETONE GLUCOSIDE", "SILANETRIOL"
                   "ROSA CENTIFOLIA EXTRACT", "ROSEMARY OLEORESIN", "SORBATE",
                   "SILICONE QUATERNIUM-16/GLYCIDOXY DIMETHICONE CROSSPOLYMER",
                   "SORBITAN", "SOY OIL", "SPIRULINA PLATENSIS EXTRACT", "TEA",
                   "STEARATE", "TEREPHTHALATE", "TETRADECANE", "THIOCTIC ACID",
                   "TRIMETHYLOLPROPANE", "TURMERIC", "UBIDECARENONE", "UREA",
                   "VANILLA OLEORESIN", "VEGETABLE WAX", "PYGEUM AFRICANUM",
                   "VITAMIN B3", "VITAMIN B6", "WITCH HAZEL", "XANTHAM GUM",
                   "VITIS VINIFERA (GRAPE) FRUIT EXTRACT", "ACRYLIC ACID",
                   "ACRYLATES/OCTYLACRYLAMIDE COPOLYMER", "ANISE ALCOHOL",
                   "ARGANIA SPINOSA OIL", "COCO-CAPRYLATE", "ETHYLHEXYL",
                   "GLUCOSAMINE", "SILANETRIOL", "WHITE SUGAR", "PALMITATE",
                   "CAPRYLATE", "LACTATE"
                   ]
fuzzyCompTox3.loc[fuzzyCompTox3.unidentified.isin(unidentifiedWrong), "match"] = False
fuzzyCompTox3.loc[fuzzyCompTox3.unidentified.isin(unidentifiedRight), "match"] = True
fuzzyCompTox3.loc[fuzzyCompTox3.identified.isin(identifiedWrong), "match"] = False

identifiedRight = ["CAPRYLIC/CAPRIC TRIGLYCERIDE", "CAPRYLYL GLYCOL",
                   "CARNAUBA WAX", "MILK PROTEIN", "MANNAN",
                   "CHROME OXIDE GREEN", "CIRE DE CARNAUBA", "CITRIC ACID",
                   "CITRONELLOL", "1,2-HEXANEDIOL", "4-BUTYLRESORCINOL", "CBD",
                   "BENZOIC ACID", "BENZOPHENONE-4", "EUGENOL", "HENNA", "MCT",
                   "BISMUTH OXYCHLORIDE", "BLACK IRON OXIDE", "HOMOSALATE",
                   "BORON NITRIDE", "BROMELAIN", "ALLANTOIN", "ILLITE", "MICA",
                   "BUTYLATED HYDROXYTOLUENE", "CAFFEINE", "CANDELILLA WAX",
                   "CANNABIDIOL", "CARBOMER 980", "CARBON BLACK",
                   "4-BUTYLRESORCINOL", "H2O2",
                   "ALOE BARBADENSIS (ALOE)LEAF JUICE", "ALOE EXTRACT", "TALC",
                   "ALOE VERA GEL", "ALUMINUM POWDER", "ANNATTO", "MENTHOL",
                   "BEESWAX", "HEPTYL UNDECYLENATE", "HEXYLENE GLYCOL",
                   "AVOBENZONE", "ASTAXANTHIN", "BASIL OIL", "BAIKAL SKULLCAP",
                   "BEHENTRIMONIUM CHLORIDE", "BLACK IRON OXIDE", "EMU OIL",
                   "CEDAR OIL", "CEDARWOOD OIL", "ZINC OXIDE", "CHOLESTEROL",
                   "CINNAMON OIL", "COUMARIN", "COCAMIDOPROPYL BETAINE",
                   "HECTORITE", "ZINC STEARATE", "COENZYME Q10",
                   "COLLOIDAL GOLD", "PECTIN", "CROCUS SATIVUS",
                   "CUPUACU BUTTER", "CYANOCOBALAMIN",
                   "CYAMOPSIS TETRAGONOLOBA", "DMDM HYDANTOIN",
                   "DECYL GLUCOSIDE", "DEHYDROACETIC ACID", "DIMETHYL SULFONE",
                   "DIATOMACEOUS EARTH", "DICALCIUM PHOSPHATE", "FLAXSEED OIL",
                   "DIHEPTYL SUCCINATE", "LACTOSE",
                   "DIISOPROPYL DIMER DILINOLEATE", "DIMETHYLSULFONE",
                   "DISTILLED WATER", "EUCALYPTUS OIL", "FLUORPHLOGOPITE",
                   "FERRIC AMMONIUM FERROCYANIDE", "GALACTOARABINAN", "LILIAL",
                   "GERMALL PLUS", "GLUCONOLACTONE", "GLYCINE BETAINE",
                   "GOLDENSEAL", "GOTU KOLA", "HEXYLRESORCINOL",
                   "HYDROLYZED PROTEIN", "INOSITOL", "HYDROXYACETOPHENONE",
                   "HYDROXYPROPYLTRIMONIUM HONEY",
                   "IODOPROPYNYL BUTYLCARBAMATE", "IRON OXIDE RED", "LAVANDIN",
                   "ISOPROPYL ALCOHOL", "ISOPROPYL MYRISTATE", "KAOLINITE",
                   "ISOSTEARIC ACID", "L-ARGININE", "LECITHIN", "MALTOL",
                   "LITSEA CUBEBA", "MAGNESIUM CHLORIDE", "MAGNESIUM STEARATE",
                   "MAGNESIUM SULFATE", "MALIC ACID", "MANDELIC ACID", "PTFE",
                   "MANGO BUTTER", "MAPLE EXTRACT", "MONTMORILLONITE",
                   "MULTANI MITTI CLAY", "NEEM EXTRACT", "NIACINAMIDE",
                   "NITROCELLULOSE", "NYLON 12", "OIL OF BERGAMOT", "PHYTOL",
                   "OLEORESIN", "ONION EXTRACT", "ORANGE PEEL",
                   "P-CRESYL METHYL ETHER", "PANTOTHENIC ACID", "PEG-20",
                   "PEG-75 LANOLIN", "PENTASODIUM PENTETATE", "PETROLATUM",
                   "PHENETHYL ALCOHOL", "PHYTIC ACID",
                   "PHYTOSTEROLS", "PINENE", "POLYBUTENE",
                   "POLYETHYLENE TEREPHTHALATE", "POLYIMIDE-1", "PROBIOTIC",
                   "POLYLACTIC ACID", "POTASSIUM HYDROXIDE", "PROPOLIS",
                   "POTASSIUM SORBATE", "PROPYL PARABEN", "BETA-GLUCAN",
                   "PRUNUS AMYGDALUS DULCIS (SWEET ALMOND) OIL", "RETINOL",
                   "PUMICE", "RED IRON OXIDE", "RETINYL PALMITATE", "SANTALOL",
                   "SAGE EXTRACT", "SALICYLIC ACID", "SCLEROTIUM GUM",
                   "SERICIN", "SILYBUM MARIANUM", "STEARALKONIUM CHLORIDE",
                   "STEARAMIDOPROPYL DIMETHYLAMINE", "STYRAX BENZOIN",
                   "SYMPHYTUM OFFICINALE", "TAPIOCA", "TIN OXIDE", "TROPOLONE",
                   "TETRAHEXYLDECYL ASCORBATE", "TITANIUM DIOXIDE", "VANILLIN",
                   "THEOBROMA GRANDIFLORUM SEED BUTTER", "TOCOPHEROLS",
                   "TOCOPHERYL ACETATE", "TOCOTRIENOL", "TRANEXAMIC ACID",
                   "TRIETHANOLAMINE", "TRIISOTRIDECYL TRIMELLITATE", "XYLITOL",
                   "TRIMETHYLPENTANEDIYL DIBENZOATE", "AVOCADO BUTTER",
                   "PYRIDOXINE HCL", "WATERCRESS OIL", "YOGURT", "XANTHAN GUM",
                   "YELLOW 11", "YUCCA EXTRACT", "ANISALDEHYDE",
                   "ALPHA OLEFIN SULFONATE", "ALPHA-BISABOLOL", "BAKUCHIOL",
                   "AMINOMETHYLPROPANOL", "AMINOPROPANOL", "CARNUBA WAX",
                   "AMMONIUM LAURYL SULFATE", "CARRAGEENAN GUM", "CERAMIDES",
                   "CITRAL", "INULIN", "FERRIC FERROCYANIDE", "TRIDECETH-12",
                   "PENTYLENE GLYCOL", "RETINAL", "THYME EXTRACT",
                   "CHROMIUM OXIDE GREEN", "ACTIVATED CHARCOAL"
                   ]
fuzzyCompTox3.loc[fuzzyCompTox3.identified.isin(identifiedRight), "match"] = True

identifiedSubstringRight = r"^(ACACIA|ALPHA[- ]ARBUTIN|CETEARETH-2|CI [1-9][0-9]{4}|BETA[- ]CAROTENE|BUTYROSPERMUM PARKII \(SHEA\)|ALPHA[ -]LIPOIC|C13-14 |C30-45 |D-[BGP]|DL[- ]PANTHENOL|GLUCONO ?DELTA ?LACTONE|GLYCERYL [CMSU]|GLYCOL (DI)?STEARATE|GUAR |HEXYL |LAURYL |LINA|MACADAMIA|METHYL[CSI]|OAT|OCT[IO]|PEG-150 (DI)?STEARATE|PHENYL.+|POLYGLYCERYL-3 (CAPRATE|DISTEARATE)|POLYQUATERNIUM|POLYSORBATE[- ]\d|QUA|SODIUM (ASCORBYL|BENZOATE|CHLORIDE|CITRATE|DEHYDROACETATE|GLUCONATE|HYDROXIDE|HYDROXYMETHYLGLYCINATE|LACTATE|LAURETH|LAURO?YL|MET[AH]|SULFATE)|SORBITAN (LAURATE|OLIVATE|STEARATE)|SQUAL[EA]NE|TETRASODIUM (EDTA|GLUTAMATE))"
identifiedOilRight = r"^(HEMPSEED|HAZELNUT|HELICHRYSUM|GRAPE SEED|BAY|BORAGE|BERGAMOT|BRAZIL NUT|APRICOT( KERNEL)?|ARGAN|GROUNDNUT|LEMON(GRASS| PEEL)?|LIME|MARULA|MINERAL|MINK|OREGANO|PUMPKIN SEED|SEA BUCKTHORN|SESAME( SEED)?|SWEET ORANGE|TEA TREE|TURMERIC|CASSIA|CLOVE|GINGER) OIL"
identifiedExtractRight = r"^(HONEY|HOPS) EXTRACT"
fuzzyCompTox3.loc[fuzzyCompTox3.identified.str.contains(identifiedSubstringRight), "match"] = True
fuzzyCompTox3.loc[fuzzyCompTox3.identified.str.contains(identifiedOilRight), "match"] = True
fuzzyCompTox3.loc[fuzzyCompTox3.identified.str.contains(identifiedExtractRight), "match"] = True

identifiedSubstringWrong = r"^(ACHILLEA MILLEFOLIUM|CETYL(?! ALCOHOL)|D[- ]LIMONENE|DIPALMITOYLETHYL|DISTEAROYLETHYL |ETHYL [MOP]|ETHYLENE|ETHYLHEXYL |GLYCERYL [DLP]|HYDROLYZED (SILK|YEAST)|LAVANDULA |L[UY]|METHYL [IM]|NER|PEG 150 [DP]|PEG-(1[02]|[46])|PENTAERYTHRITYL|POLYGLYCERYL-([24]|3 DIISO)|POLYMETHYL|PPG|PRO(LI|PA)NE(?!DIOL)|SODIUM (ACRYLATE|COCO SULFATE|MAGNESIUM|POLY)|SORBITAN OLEATE |STEARETH|TETRASODIUM (ETIDRONATE|PYROPHOSPHATE)|TRIGLYCERIDES?|COCODIMONIUM|ISO[CD]E[TC]YL)"
identifiedHydrogenatedWrong = r"^HYDROGENATED (PALM|VEGETABLE|COCONUT|MICROCRYSTALLINE)"
fuzzyCompTox3.loc[fuzzyCompTox3.identified.str.contains(identifiedSubstringWrong), "match"] = False
fuzzyCompTox3.loc[fuzzyCompTox3.identified.str.contains(identifiedHydrogenatedWrong), "match"] = False

fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ACETIC ACID") & fuzzyCompTox3.unidentified.str.contains(r"[A-Z]ACETIC ACID"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ACETIC ACID") & fuzzyCompTox3.unidentified.str.contains(r"(?<![A-Z])ACETIC ACID"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ALCOHOL") & fuzzyCompTox3.unidentified.str.contains("\bSDA?\b"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ALCOHOL") & fuzzyCompTox3.unidentified.str.contains("(?<!ETH)YL ALCOHOL"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ALUMINUM") & fuzzyCompTox3.unidentified.str.contains(r"LAKE|POLYMER|STARCH|SILICA|CITRUS|POWDER"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ALUMINUM") & ~fuzzyCompTox3.unidentified.str.contains(r"LAKE|POLYMER|STARCH|SILICA|CITRUS|POWDER"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "AMODIMETHICONE") & fuzzyCompTox3.unidentified.str.contains(r"\bAMODIMETHICONE\b") & ~fuzzyCompTox3.unidentified.str.contains("PG"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "AMODIMETHICONE") & (~fuzzyCompTox3.unidentified.str.contains(r"\bAMODIMETHICONE\b") | fuzzyCompTox3.unidentified.str.contains("PG")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ASCORBIC ACID") & fuzzyCompTox3.unidentified.str.contains(r"(?<!L-)ASCORBIC ACID"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ASCORBIC ACID") & fuzzyCompTox3.unidentified.str.contains(r"(?<=L-)ASCORBIC ACID"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ASCORBYL PALMITATE") & fuzzyCompTox3.unidentified.str.contains("ASCORBYL PALMITATE"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ASCORBYL PALMITATE") & ~fuzzyCompTox3.unidentified.str.contains("ASCORBYL PALMITATE"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BEET SUGAR") & fuzzyCompTox3.unidentified.str.contains("BETAINE|TRIMETHYLGLYCINE"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BEET SUGAR") & ~fuzzyCompTox3.unidentified.str.contains("BETAINE|TRIMETHYLGLYCINE") & (fuzzyCompTox3.unidentified != "SUGAR"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BENTONITE") & ~fuzzyCompTox3.unidentified.str.contains(r"NIUM") & ~fuzzyCompTox3.unidentified.str.contains(r"MONTMORILLONITE|BENTONITE CLAY"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BENTONITE") & ~fuzzyCompTox3.unidentified.str.contains(r"NIUM") & fuzzyCompTox3.unidentified.str.contains(r"MONTMORILLONITE|BENTONITE CLAY"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BENTONITE CLAY") & ~fuzzyCompTox3.unidentified.str.contains(r"NIUM") & ~fuzzyCompTox3.unidentified.str.contains(r"MONTMORILLONITE"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BENTONITE CLAY") & ~fuzzyCompTox3.unidentified.str.contains(r"NIUM") & fuzzyCompTox3.unidentified.str.contains(r"MONTMORILLONITE"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BETAINE") & fuzzyCompTox3.unidentified.str.contains(r"(PROPYL |LAURYL |COCO-|GLYCINE )BETAINE"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BETAINE") & ~fuzzyCompTox3.unidentified.str.contains(r"(PROPYL |LAURYL |COCO-|GLYCINE )BETAINE"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BLUE 1") & fuzzyCompTox3.unidentified.str.contains(r"(?<!FD&C )BLUE 1(?! LAKE)") & ~fuzzyCompTox3.unidentified.str.contains("CI 42090"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BLUE 1") & (fuzzyCompTox3.unidentified.str.contains(r"BLUE 1 LAKE|FD&C BLUE 1") | fuzzyCompTox3.unidentified.str.contains("CI 42090")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BLUE 1 LAKE") & fuzzyCompTox3.unidentified.str.contains(r"BLUE 1 LAKE") & ~fuzzyCompTox3.unidentified.str.contains("CI 42090"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BLUE 1 LAKE") & (fuzzyCompTox3.unidentified.str.contains(r"BLUE 1(?! LAKE)") | fuzzyCompTox3.unidentified.str.contains("CI 42090")), "match"] = False

fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "BUTTER") & ~fuzzyCompTox3.unidentified.str.contains(r"MILK|GHEE|DAIRY"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "CALCIUM") & fuzzyCompTox3.unidentified.str.contains(r"^CALCIUM(?! MONTMORILLONITE)"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "CALCIUM") & (~fuzzyCompTox3.unidentified.str.contains(r"^CALCIUM") | fuzzyCompTox3.unidentified.str.contains("CALCIUM(?! MONTMORILLONITE)")), "match"] = False
fuzzyCompTox3.loc[fuzzyCompTox3.identified.str.contains(r"CAMELLIA SINENSIS [A-Z]"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "CASTOR OIL") & ~fuzzyCompTox3.unidentified.str.contains(r"HYDROGENATED|PEG|CASTOR OIL DERIVED"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "CELLULOSE") & fuzzyCompTox3.unidentified.str.contains(r"(?<![A-Z])CELLULOSE(?! NITRATE)"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "CELLULOSE") & fuzzyCompTox3.unidentified.str.contains(r"(?<=[A-Z])CELLULOSE"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "CITRATE") & fuzzyCompTox3.unidentified.str.contains(r"(?<= )CITRATE"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "CITRATE") & fuzzyCompTox3.unidentified.str.contains(r"(?<! )CITRATE"), "match"] = True
fuzzyCompTox3.loc[fuzzyCompTox3.identified.str.contains("COCONUT (BUTTER|EXTRACT)") & fuzzyCompTox3.unidentified.str.contains("COCONUT (BUTTER|EXTRACT)"), "match"] = True
fuzzyCompTox3.loc[fuzzyCompTox3.identified.str.contains("COCONUT (BUTTER|EXTRACT)") & ~fuzzyCompTox3.unidentified.str.contains("COCONUT (BUTTER|EXTRACT)"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "COCONUT OIL") & fuzzyCompTox3.unidentified.str.contains("(FROM( [A-Z]+)?|FR?ACTIONATED|FRACTIONED) COCONUT OIL"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "COCONUT OIL") & fuzzyCompTox3.unidentified.str.contains("COCONUT OIL DERIVED"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "COCONUT OIL") & fuzzyCompTox3.unidentified.str.contains("TRIGLYCERIDE|COC[OA]MIDOPROPYL BETAINE|SODIUM COCOAMPHOACETATE|(POTASSIUM|SODIUM) COCOATE|GLYCERIN|FRACTIONATED"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "COCONUT OIL") & fuzzyCompTox3.match.isna() & ~fuzzyCompTox3.unidentified.str.contains("SAPONIFIED"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "COCOS NUCIFERA OIL") & fuzzyCompTox3.unidentified.str.contains("COCOS NUCIFERA OIL") & ~fuzzyCompTox3.unidentified.str.contains("COCONUT OIL"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "COCOS NUCIFERA OIL") & ~fuzzyCompTox3.unidentified.str.contains("COCOS NUCIFERA OIL"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "COLLAGEN") & ~fuzzyCompTox3.unidentified.str.contains("DIMETHYLSULFONE"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "DIMETHICONE") & fuzzyCompTox3.unidentified.str.contains(r"PEG|PPG|VINYL|AMODIMETHICONE"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "DIMETHICONE") & fuzzyCompTox3.unidentified.str.contains(r"\bDIMETHICONE\b") & ~fuzzyCompTox3.unidentified.str.contains(r"PEG|PPG|VINYL|POLYMER"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "DIMETHICONOL") & (fuzzyCompTox3.unidentified == "POLYSILICONE-11,DIMETHICONOL"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "DIMETHICONOL") & (fuzzyCompTox3.unidentified != "POLYSILICONE-11,DIMETHICONOL"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "DMAE") & fuzzyCompTox3.unidentified.str.contains("BITARTRATE"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "DMAE") & ~fuzzyCompTox3.unidentified.str.contains("BITARTRATE"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "EPSOM SALT") & fuzzyCompTox3.unidentified.str.contains("EPSOM") & ~fuzzyCompTox3.unidentified.str.contains("MAGNESIUM SULFATE"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "EPSOM SALT") & (~fuzzyCompTox3.unidentified.str.contains("EPSOM") | fuzzyCompTox3.unidentified.str.contains("MAGNESIUM SULFATE")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "FRUIT SUGAR") & (fuzzyCompTox3.unidentified == "SUGAR"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "FRUIT SUGAR") & (fuzzyCompTox3.unidentified != "SUGAR"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "GLUCOSE") & (fuzzyCompTox3.unidentified == "SUGAR (GLUCOSE"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "GLUCOSE") & (fuzzyCompTox3.unidentified != "SUGAR (GLUCOSE"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "GLYCERIN") & ~fuzzyCompTox3.unidentified.str.contains(r"\bGLYCERIN\b"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "GLYCERIN") & fuzzyCompTox3.unidentified.str.contains("ETHYL|HEX[EY]L|LAUR[EY]L|COPOLYMER|FROM VEGETABLE GLYCERIN|CAPRYLOYL|GLYCEROL|GLYCERIN (FATTY ACID ESTER|BASED)"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "GLYCERIN") & fuzzyCompTox3.match.isna(), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "GOLD") & (~fuzzyCompTox3.unidentified.str.contains(r"\bGOLD\b") | fuzzyCompTox3.unidentified.str.contains(r"^[A-Z]+ [A-Z]+ [0-9]{3}$") | (fuzzyCompTox3.unidentified.str.contains("GOLD") & fuzzyCompTox3.unidentified.str.contains("MICA|CLAY|ASTRAL"))), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "GOLD") & fuzzyCompTox3.match.isna(), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "OLEIC ACID") & (fuzzyCompTox3.unidentified.str.contains(r"\bOLEIC ACID\b") & ~fuzzyCompTox3.unidentified.str.contains("PEG")), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "OLEIC ACID") & ~(fuzzyCompTox3.unidentified.str.contains(r"\bOLEIC ACID\b") & ~fuzzyCompTox3.unidentified.str.contains("PEG")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "RICE BRAN OIL") & fuzzyCompTox3.unidentified.str.contains("RICE BRAN OIL"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "RICE BRAN OIL") & ~fuzzyCompTox3.unidentified.str.contains("RICE BRAN OIL"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SALT") & (~fuzzyCompTox3.unidentified.str.contains(r"\bSALT\b") | fuzzyCompTox3.unidentified.str.contains(r"\bSEA SALT\b")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SALT") & fuzzyCompTox3.unidentified.str.contains(r"\bSALT\b") & ~fuzzyCompTox3.unidentified.str.contains(r"\bSEA SALT\b"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SEAWEED EXTRACT") & ~fuzzyCompTox3.unidentified.str.contains("CARRAGEENAN") & fuzzyCompTox3.unidentified.str.contains("SEAWEED"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SEAWEED EXTRACT") & (fuzzyCompTox3.unidentified.str.contains("CARRAGEENAN") | ~fuzzyCompTox3.unidentified.str.contains("SEAWEED")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SILICA") & fuzzyCompTox3.unidentified.str.contains(r"\bS?ILICA\b") & ~fuzzyCompTox3.unidentified.str.contains("DIMETHYL"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SILICA") & (~fuzzyCompTox3.unidentified.str.contains(r"\bS?ILICA\b") | fuzzyCompTox3.unidentified.str.contains("DIMETHYL")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SUNFLOWER OIL") & fuzzyCompTox3.unidentified.str.contains("SUNFLOWER OIL") & ~fuzzyCompTox3.unidentified.str.contains(r"HELIANTHUS ANNUUS|DERIVED FROM WHOLE NON-GMO SUNFLOWER OIL"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SUNFLOWER OIL") & (~fuzzyCompTox3.unidentified.str.contains("SUNFLOWER OIL") | fuzzyCompTox3.unidentified.str.contains(r"HELIANTHUS ANNUUS|DERIVED FROM WHOLE NON-GMO SUNFLOWER OIL")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SUNFLOWER SEED OIL") & fuzzyCompTox3.unidentified.str.contains("SUNFLOWER SEED OIL"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "SUNFLOWER SEED OIL") & ~fuzzyCompTox3.unidentified.str.contains("SUNFLOWER SEED OIL"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "WATER") & fuzzyCompTox3.unidentified.str.contains(r"\bWATER\b") & fuzzyCompTox3.unidentified.str.contains(r"DISTILL|FILTERED|PURIFI?ED(?! ALOE)|ALKALINE|STERILIZED|MINERAL WATER|DE-?IONI[SZ]ED|INGREDIENTS|INFUSED|SPRING|BUTYLENE GLYCOL|ALL PLANT DERIVED|GLYCEROL AMINO ACIDS|HYDROXYHYDROCINNAMATE"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "WATER") & fuzzyCompTox3.unidentified.str.contains(r"(: |\()WATER"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "WATER") & fuzzyCompTox3.match.isna(), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "YELLOW 5 LAKE") & fuzzyCompTox3.unidentified.str.contains(r"\bYELLOW 5 LAKE\b") & ~fuzzyCompTox3.unidentified.str.contains("CI 19140"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "YELLOW 5 LAKE") & (~fuzzyCompTox3.unidentified.str.contains(r"\bYELLOW 5 LAKE\b") | fuzzyCompTox3.unidentified.str.contains("CI 19140")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "YELLOW 6 LAKE") & fuzzyCompTox3.unidentified.str.contains(r"\bYELLOW 6 LAKE\b") & ~fuzzyCompTox3.unidentified.str.contains("CI 15985"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "YELLOW 6 LAKE") & (~fuzzyCompTox3.unidentified.str.contains(r"\bYELLOW 6 LAKE\b") | fuzzyCompTox3.unidentified.str.contains("CI 15985")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "FD&C BLUE 1") & fuzzyCompTox3.unidentified.str.contains("FD&C BLUE 1") & ~fuzzyCompTox3.unidentified.str.contains("CI 42090"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "FD&C BLUE 1") & (~fuzzyCompTox3.unidentified.str.contains("FD&C BLUE 1") | fuzzyCompTox3.unidentified.str.contains("CI 42090")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "FD&C YELLOW 5") & fuzzyCompTox3.unidentified.str.contains("FD&C YELLOW 5") & ~fuzzyCompTox3.unidentified.str.contains("CI 19140"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "FD&C YELLOW 5") & (~fuzzyCompTox3.unidentified.str.contains("FD&C YELLOW 5") | fuzzyCompTox3.unidentified.str.contains("CI 19140")), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "ACACIA DECURRENS FLOWER WAX") & (fuzzyCompTox3.unidentified == "FLOWER"), "match"] = False
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "COCONUT FATTY ACIDS") & fuzzyCompTox3.unidentified.str.contains("COCONUT FATTY ACIDS") & ~fuzzyCompTox3.unidentified.str.contains("FOAM STABILIZER DERIVED FROM"), "match"] = True
fuzzyCompTox3.loc[(fuzzyCompTox3.identified == "COCONUT FATTY ACIDS") & (~fuzzyCompTox3.unidentified.str.contains("COCONUT FATTY ACIDS") | fuzzyCompTox3.unidentified.str.contains("FOAM STABILIZER DERIVED FROM")), "match"] = False

identifiedInclude = ["AHA", "AMINO ACIDS", "AMP", "AQUA",
                     "ARACHIDYL GLUCOSIDE", "BILBERRY FRUIT EXTRACT",
                     "BLADDERWRACK EXTRACT", "BLUE GREEN ALGAE", "BRAN",
                     "CALENDULA OFFICINALIS FLOWER EXTRACT",
                     "CAMELLIA SINENSIS", "CETEARYL ALCOHOL",
                     "CETRIMONIUM CHLORIDE", "CETYL ALCOHOL",
                     "CHAMOMILE EXTRACT", "CITRUS SINENSIS OIL",
                     "COCO-BETAINE", "COCAMIDE MEA", "COCOAMIDOPROPYL BETAINE",
                     "CORN OIL", "CORN STARCH", "CORNSTARCH",
                     "CUCUMBER EXTRACT", "CURCUMIN", "DENATURED ALCOHOL",
                     "DICETYLDIMONIUM CHLORIDE", "ETHYL ALCOHOL",
                     "DISODIUM COCOAMPHODIACETATE", "EVENING PRIMROSE OIL",
                     "FRANKINCENSE OIL", "GARLIC EXTRACT", "GERANIOL",
                     "GLYCERYL OLEATE", "HONEYSUCKLE EXTRACT",
                     "HORSETAIL PLANT EXTRACT", "IRISH MOSS EXTRACT",
                     "HYDROGENATED JOJOBA OIL", "ISOHEXADECANE",
                     "HYDROXYETHYLCELLULOSE", "ISODODECANE", "ISOLEUCINE",
                     "ISOPROPYL PALMITATE", "LIMONENE", "JASMINE ABSOLUTE",
                     "LICORICE ROOT EXTRACT", "LINOLEIC ACID",
                     "MANGANESE VIOLET", "MELALEUCA ALTERNIFOLIA LEAF OIL",
                     "MENTHA PIPERITA OIL", "MICROCRYSTALLINE WAX",
                     "METHYLHEPTYL ISOSTEARATE", "MULBERRY EXTRACT",
                     "PALM KERNEL OIL", "PALMAROSA OIL", "PEG-14",
                     "PALMITOYL OLIGOPEPTIDE", "PALMITOYL PENTAPEPTIDE-4",
                     "PEG-150 PENTAERYTHRITYL TETRASTEARATE", "SAFFLOWER OIL"
                     "PENTYLENE GLYCOL", "PHENOXYETHANOL", "PINE BARK EXTRACT",
                     "POTASSIUM OLEATE", "PRUNUS AMYGDALUS DULCIS OIL",
                     "PRUNUS ARMENIACA (APRICOT) KERNEL OIL",
                     "PRUNUS ARMENIACA OIL", "PUNICA GRANATUM EXTRACT",
                     "RED 30 LAKE", "RED 40 LAKE", "RICE STARCH", "ROSE OIL",
                     "ROSMARINUS OFFICINALIS LEAF EXTRACT",
                     "SACCHARUM OFFICINARUM EXTRACT", "SAFFLOWER SEED OIL",
                     "SODIUM COCOYL GLUTAMATE", "SODIUM COCOYL ISETHIONATE",
                     "SODIUM OLEATE", "SODIUM STEAROYL LACTYLATE",
                     "SORBITAN OLEATE", "SPEARMINT OIL",
                     "SUCROSE COCOATE",
                     "SUNFLOWER (HELIANTHUS ANNUUS) SEED OIL",
                     "ULTRAMARINE BLUE", "VITIS VINIFERA SEED EXTRACT"
                     ]
for substring in identifiedInclude:
    unidentifiedSubstring = r"\b" + substring + r"\b"
    fuzzyCompTox3.loc[(fuzzyCompTox3.identified == substring) & fuzzyCompTox3.unidentified.str.contains(unidentifiedSubstring), "match"] = True
    fuzzyCompTox3.loc[(fuzzyCompTox3.identified == substring) & ~fuzzyCompTox3.unidentified.str.contains(unidentifiedSubstring), "match"] = False

identifiedIncludeDict = {"AMBER": r"EXFO|VANILLA OIL",
                         "APPLE EXTRACT": r"\bAPPLE\b",
                         "BEHENTRIMONIUM METHOSULFATE": "BEHENTRIMONIUM",
                         "BUTTER": "GHEE", "CALENDULA EXTRACT": "CALENDULA",
                         "CANANGA ODORATA OIL": "CANANGA",
                         "CARROT EXTRACT": "CARROT",
                         "CHICKWEED EXTRACT": "CHICKWEED",
                         "COFFEE EXTRACT": "COFFEE",
                         "COPAIBA EXTRACT": "COPAIBA",
                         "CRANBERRY EXTRACT": "CRANBERRY",
                         "DANDELION EXTRACT": "DANDELION",
                         "DISODIUM COCOAMPHODIPROPIONATE": "AMPHODIPROPIONATE",
                         "ETHANOLAMINE": "MONOETHANOLAMINE",
                         "HIBISCUS SABDARIFFA FLOWER EXTRACT": "HIBISCUS SABDARIFFA",
                         "HORSETAIL EXTRACT": "HORSETAIL",
                         "LICORICE EXTRACT": "LICORICE",
                         "LIMNANTHES ALBA (MEADOWFOAM) SEED OIL": "LIMNANTHES ALBA",
                         "MAGNESIUM": r"MAGNESIUM- [0-9]|OIL",
                         "MYRISTATE": "MYRISTATE ACID", "NEEM OIL": "NEEM OIL",
                         "NEEM SEED OIL": "NEEM SEED OIL",
                         "OENOTHERA BIENNIS (EVENING PRIMROSE) OIL": r"OENOTHERA|BIENNIS",
                         "OIL OF ROSEMARY": "ROSEMARY",
                         "PAPAYA EXTRACT": "PAPAYA",
                         "PATCHOULI EXTRACT": "PATCHOULI",
                         "POTASSIUM COCOATE": "COCOATE",
                         "SODIUM COCOATE": "COCOATE", "SODIUM EDTA": "TETRADI",
                         "SPEARMINT": r"SPEARMINT(?! OIL)",
                         "VANILLA": "SCENT|FRAGRANCE",
                         "VITIS VINIFERA (GRAPE) SEED EXTRACT": r"VITIS VINIFERA \(GRAPE",
                         "VITIS VINIFERA SEED OIL": "VITIS VINIFER",
                         "WHEAT GERM OIL": r"(?<!DERIVED FROM )WHEAT GERM OIL",
                         "YELLOW IRON OXIDE": "YELLOW IRON OXIDE", "ZINC": r"ZINC (&|\d)"}
for identified, unidentified in identifiedIncludeDict.items():
    fuzzyCompTox3.loc[(fuzzyCompTox3.identified == identified) & fuzzyCompTox3.unidentified.str.contains(unidentified), "match"] = True
    fuzzyCompTox3.loc[(fuzzyCompTox3.identified == identified) & ~fuzzyCompTox3.unidentified.str.contains(unidentified), "match"] = False

elementPercent = ["POTASSIUM", "SODIUM", "TITANIUM"]
for element in elementPercent:
    unidentifiedRegex = r"\b" + element + r"\b- \d"
    fuzzyCompTox3.loc[(fuzzyCompTox3.identified == element) & fuzzyCompTox3.unidentified.str.contains(unidentifiedRegex), "match"] = True
    fuzzyCompTox3.loc[(fuzzyCompTox3.identified == element) & ~fuzzyCompTox3.unidentified.str.contains(unidentifiedRegex), "match"] = False

identifiedExcludeDict = {"ACTIVATED CARBON": "ACTIVATED CHARCOAL",
                         "ACRYLAMIDE COPOLYMER": r"^COPOLYMER$",
                         "ACRYLATE COPOLYMER": r"^COPOLYMER$",
                         "ALGAE EXTRACT": "BLUE GREEN ALGAE",
                         "ALMOND OIL": "SWEET ALMOND OIL",
                         "ALOE": r"BARBADENSIS|VERA",
                         "ALOE BARBADENSIS": "ALOE VERA GEL",
                         "ALOE VERA": "ALOE VERA GEL", "CANE SUGAR": "SUCROSE",
                         "ARBUTIN": r"(ALPHA |A-)ARBUTIN",
                         "ARGANIA SPINOSA KERNEL OIL": "ARGAN OIL",
                         "ARGININE": "L-ARGININE",
                         "ARNICA MONTANA EXTRACT": r"^EXTRACT$",
                         "AMINO ACID": r"AMINO ACIDS|SODIUM PCA",
                         "AVOCADO OIL": r"HYDROGENATED|SAPONIFIED",
                         "BABASSU OIL": "SAPONIFIED",
                         "BAKING SODA": "SODIUM BICARBONATE",
                         "BISABOLOL": "ALPHA-BISABOLOL", "BIOTIN": "D-BIOTIN",
                         "BLADDERWRACK": "BLADDERWRACK EXTRACT",
                         "CERA ALBA": "BEESWAX",
                         "CAPRIC TRIGLYCERIDE": r"CAPRYLIC",
                         "CAPRYLIC TRIGLYCERIDE": "CAPRIC", "CARAMEL": "D&C",
                         "CARBOMER": r"SODIUM|CARBOMER 980",
                         "CARMINE": "CI 75470",
                         "CARRAGEENAN": r"CARRAGEENAN GUM|IRISH MOSS EXTRACT",
                         "CETEARETH": r"CETEARETH-2[05]",
                         "CHARCOAL": r"ACTIVATED (CHARCOAL|CARBON)",
                         "CHAMOMILE OIL": "DERIVED FROM CHAMOMILE OIL",
                         "CHROMIUM OXIDE": "CHROMIUM OXIDE GREEN",
                         "CINNAMON": r"LOUREIROI|ZEYLANICUM|CULILAWAN|CASSIA|OIL",
                         "COCOA BUTTER": r"(^COCO$)|STEARIC ACID",
                         "COCONUT FATTY ACID": r"COCAMIDE MEA|CAPRYLYL GLYCOL|(^COCO$)|FOAM STABILIZER",
                         "COMFREY": r"SYMPHYTUM OFFICINALE|ALLANTOIN",
                         "COPPER": r"PEPTIDE|CHLOROPHYLLIN|SACCHAROMYCES|DISODIUM EDTA COPPER",
                         "CLAY": r"KAOLIN|HECTORITE|BENTONITE|IRON OXIDE|ILLITE|ZEOLIT",
                         "CYAMOPSIS TETRAGONOLOBA": "GUAR GUM",
                         "DISODIUM EDTA": "TETRA|COPPER", "D&C YELLOW 5": "CI 19140",
                         "EMULSIFIER": r"ALCOHOL|BTMS|GLYCERYL|POLYSORBATE|SORBITAN|CASTOR OIL|GLUCOSIDE|SORBITOL|POLYGLYCERYL|BEHEN?TRIMONIUM|TRIGLYCERIDE|CETEARYL",
                         "ETHANOL": "DENATURED ALCOHOL|[A-Z]ETHANOL|ETHANOLAMINE",
                         "GLYCERINE": r"ETHYL|MADE FROM VEGETABLE GLYCERINE",
                         "GLYCEROL": "ESTER", "GLYCINE SOJA": "PEG",
                         "GLYCOLIC ACID": "POLYMER", "GRAPEFRUIT OIL": "LOVE",
                         "GREEN TEA EXTRACT": r"GREEN TEA|(^EXTRACT$)",
                         "GUM ARABIC": "ACACIA SENEGAL GUM",
                         "HELIANTHUS ANNUUS": r"\b(WAX|CERA|SAPONIFIED|SUNFLOWER SEED OIL)\b",
                         "HONEY": r"QUAT|HONEY[A-Z]",
                         "HYDROGENATED CASTOR OIL": r"P[PE]G|POLYMER|(^HYDROGENATED$)",
                         "HYALURONIC ACID": r"HYALURONIC ACID \( ?SODIUM HYALURONATE",
                         "IRISH MOSS": r"CARRAGEENAN|IRISH MOSS EXTRACT",
                         "IRON": "OXIDE",
                         "IRON OXIDE": r"RED|YELLOW|BLACK|IRON OXIDES|[1-9][0-9]{4}",
                         "IRON OXIDES": r"RED|YELLOW|BLACK|IRON OXIDE\b|[1-9][0-9]{4}",
                         "JOJOBA OIL": r"HYDROGENATED|SAPONIFIED",
                         "KAOLIN": r"KAOLINITE|MONTMORILLONITE",
                         "KELP": "ALGAE EXTRACT",
                         "KERATIN": r"FERMENT|VEGEKERATIN",
                         "L-ASCORBIC ACID": "ETHYL",
                         "LANOLIN": r"PEG|HYDROXYLATED",
                         "LAVENDER": r"EXTRACT|WATER|HYDROSOL|POWDER|SPIKE LAVENDER|FLOWER",
                         "LEMON": r"BALM|FERMENT|LEMONGRASS|VERBENA|LEMON( PEEL)? OIL|NATURAL LEMON FRAGRANCE",
                         "MALTODEXTRIN": "COPOLYMER",
                         "MILK": r"GOAT|ALMOND|SOY|OAT|COCONUT|MILK SUGAR|RICE",
                         "OLIVE OIL": "SAPONIFIED|OLIVE OIL GIRLS|MADE WITH ORGANIC OLIVE OIL|SQUALENE",
                         "PALM OIL": r"DERIVED|CETYL|SAPONIFIED|(^PALM$)",
                         "PANTHENOL": r"DL?[- ]PANTHENOL",
                         "PAPAIN": "PAPAYA ENZYME",
                         "PAPAYA ENZYME": r"(^PAPAYA$)|FERMENT",
                         "PEG": r"CASTOR|DIMETHICONE|STEARATE|PPG|HYDROGENATED|MEADOWFOAMATE",
                         "PEG-8": r"DIMETHICONE|MEADO", "PENTYLENE": "GLYCOL",
                         "PEPPERMINT": r"(MENTHA PIPERITA|PEPPERMINT) OIL|(^MINT$)",
                         "PEPPERMINT LEAF OIL": r"^MINT$",
                         "PEPPERMINT OIL": r"MENTHA PIPERITA OIL|(^MINT$)|PULEGONE",
                         "PHOSPHOLIPIDS": "LECITHIN",
                         "POLYISOBUTENE": "HYDROGENATED", "POLYSORBATE": r"\d",
                         "PROPANEDIOL": "3-PROPANEDIOL",
                         "PROPYLENE GLYCOL": r"POLYMER|PROPYLENE GLYCOLIC",
                         "PROVITAMIN B5": "PANTHENOL",
                         "RED 22 LAKE": r"CI 45380|(^LAKE$)",
                         "RED 28 LAKE": r"CI 45410|(^LAKE$)", "RED 30": "LAKE",
                         "RED 7": "CI 15850", "SAFFLOWER OIL": r"(^FLOWER( OIL)?$)|GLYCERINE", "SEA SALT": "SODIUM CHLORIDE",
                         "SHEA BUTTER": r"PEG|SAPONIFIED|BETAINE|BUTTERATE|HYDROGENATED",
                         "SILVER": r"DIHYDROGEN|CITRATE",
                         "SODIUM BICARBONATE": "DOES NOT CONTAIN",
                         "SODIUM HYALURONATE": "POLYMER",
                         "SORBIC ACID": "VITAMIN C",
                         "SORBITOL": r"ESTER|D-GLUCITOL",
                         "SOYBEAN OIL": "GLYCINE SOJA", "STARCH": r"PHOSPHATE|(CORN|RICE) ?STARCH",
                         "STEARIC ACID": r"ISOSTEARIC|DERIVED FROM NATURAL STEARIC ACID",
                         "STEARYL ALCOHOL": "CETYL",
                         "SUCROSE": "SUCROSE COCOATE",
                         "SUNFLOWER SEED": r"OIL|(^FLOWER$)|HELIANTHUS ANNUUS",
                         "SWEET ALMOND OIL": "PRUNUS AMYGDALUS DULCIS OIL",
                         "THYME": "THYME EXTRACT",
                         "TOCOPHEROL": r"TOCOPHEROLS|ACETATE",
                         "TRIDECETH-6": r"PPG- ?1",
                         "TRIMETHYLGLYCINE": "BETAINE",
                         "UBIQUINONE": "COENZYME Q10", "ULTRAMARINE": "BLUE",
                         "VEGETABLE OIL": r"TRIGLYCERIDE|OLEATE|OILS|VEGETABLE OIL SOURCE",
                         "VEGETABLE OILS": r"(^OILS$)|OLEATE|SAPONIFIED",
                         "VINEGAR": "ACETIC ACID",
                         "VITAMIN A": r"BETA[- ]CAROTENE|RETINOL|RETINYL PALMITATE",
                         "VITAMIN B": r"VITAMIN B(5|12)|PYRIDOXINE|PANTHENOL|NIACINAMIDE|BIOTIN|CYANOCOBALAMIN",
                         "VITAMIN B5": r"PANTHENOL|PANTOTHENIC ACID",
                         "VITAMIN B12": "CYANOCOBALAMIN",
                         "VITAMIN C": r"ASCORBIC ACID|TETRAHEXYLDECYL ASCORBATE|ASCORBYL (PALMITATE|PHOSPHATE)|PRO-VITAMIN C",
                         "VITAMIN E": r"TOCOPHEROL|TOCOTRIENOL|TOCOPHERYL ACETATE",
                         "XANTHAN": "XANTHAN GUM"
                         }
for identified, unidentified in identifiedExcludeDict.items():
    fuzzyCompTox3.loc[(fuzzyCompTox3.identified == identified) & fuzzyCompTox3.unidentified.str.contains(unidentified), "match"] = False
    fuzzyCompTox3.loc[(fuzzyCompTox3.identified == identified) & ~fuzzyCompTox3.unidentified.str.contains(unidentified), "match"] = True

unidentifiedIncludeDict = {"ISOSTEARATE": "ISOSTEARYL ISOSTEARATE",
                           "ISOSTEARYL": "ISOSTEARYL ISOSTEARATE",
                           "ISOLEUCINE (AMINO ACID": "ISOLEUCINE",
                           "CITRUS": "CITRUS EXTRACT",
                           "OLEA EUROPAEA": "FRUIT OIL",
                           "PRUNUS": "PRUNUS AMYGDALUS OIL",
                           "ROSE": r"\bROSE\b", "ROSEMARY": "OIL OF ROSEMARY",
                           "CAPRYLYL": "CAPRYLYL GLYCOL", "CANE": "CANE SUGAR",
                           "LAMINARIA": "LAMINARIA EXTRACT"
                           }
for unidentified, identified in unidentifiedIncludeDict.items():
    fuzzyCompTox3.loc[(fuzzyCompTox3.unidentified == unidentified) & fuzzyCompTox3.identified.str.contains(identified), "match"] = True
    fuzzyCompTox3.loc[(fuzzyCompTox3.unidentified == unidentified) & ~fuzzyCompTox3.identified.str.contains(identified), "match"] = False

fuzzyCompTox3stillUnidentified = fuzzyCompTox3.query("match.isna()")
fuzzyCompTox3stillUnidentified.loc[(fuzzyCompTox3stillUnidentified.identified == "ALCOHOL") & fuzzyCompTox3stillUnidentified.unidentified.str.contains(r"SDA?[- ]ALCOHOL|SUGAR CANE|\bDENAT\b"), "match"] = True
fuzzyCompTox3stillUnidentified.loc[(fuzzyCompTox3stillUnidentified.identified == "ALCOHOL") & fuzzyCompTox3stillUnidentified.match.isna(), "match"] = False
identifiedWrong2 = ["CASTOR OIL", "COCONUT OIL", "COCOS NUCIFERA OIL",
                    "DIMETHICONE"]
fuzzyCompTox3stillUnidentified.loc[fuzzyCompTox3stillUnidentified.identified.isin(identifiedWrong2), "match"] = False
fuzzyCompTox3stillUnidentified.loc[fuzzyCompTox3stillUnidentified.match.isna(), "match"] = False

fuzzyCompTox3identified = fuzzyCompTox3.query("match.notna()")
fuzzyCompTox3 = pd.concat([fuzzyCompTox3identified, fuzzyCompTox3stillUnidentified], ignore_index=True)
# %%
"""Now, to find ingredient names that are really comprised of multiple
ingredients. I'm going to do this using 2 ways

1. unidentified ingredient names that are matched to multiple identified
ingredients
2. ingredient names with certain substrings such as 'and'
"""

fuzzyCompTox3correct = (fuzzyCompTox3.query("match == True")
                        .drop(columns=["match"])
                        )
fuzzyCompTox3duplicate1 = fuzzyCompTox3correct.loc[fuzzyCompTox3correct.unidentified.duplicated(False)]
notFullyIdentified = ["LINALOOL GERANIOL BORNYL ACETATE",
                      "PHENOXYETHANOL (AND) CAPRYLYL GLUCOL (AND) SORBIC ACID"]

andRegex = r"&|AND|\+|/|INFUSION OF|INFUSED WITH"
fuzzyCompTox3duplicate2 = fuzzyCompTox3correct.loc[fuzzyCompTox3correct.unidentified.str.contains(andRegex)]
fuzzyCompToxDuplicates = fuzzyCompTox3duplicate1.merge(fuzzyCompTox3duplicate2, "outer", ["unidentified", "identified", "lengthsRatio", "partialRatio", "unidentifiedLength", "identifiedLength"], indicator=True)
fuzzyCompTox3duplicate2toCheck = (fuzzyCompToxDuplicates.query("_merge == 'right_only'")
                                  .drop(columns=["_merge"])
                                  )
fuzzyCompTox3duplicate2toCheck_multipleIdentified = fuzzyCompTox3duplicate2toCheck.loc[fuzzyCompTox3duplicate2toCheck.unidentified.duplicated(False)]

"""After manually inspecting some of the unidentified names from the 2nd
method, I see that some of these names contain multiple ingredients. However, I
don't give a fuck at this point. I'll just, start fuzzy string matching with
CosIng."""

fuzzyCompTox3correct = fuzzyCompTox3correct.loc[~fuzzyCompTox3correct.unidentified.isin(notFullyIdentified)]
fuzzyCompTox3correct = (fuzzyCompTox3correct.filter(["unidentified", "identified"])
                        .drop_duplicates()
                        )

fuzzyCompToxIdentified = pd.concat([fuzzyCompTox1identified, fuzzyCompTox2identified, fuzzyCompTox3correct], ignore_index=True)

unidentifiedDF3 = (unidentifiedDF2.merge(fuzzyCompTox3correct, "left", left_on="allCaps", right_on="unidentified")
                   .query("unidentified.isna()")
                   .filter(["ingredientName", "allCaps"])
                   )

fuzzyCompTox3wrong = (fuzzyCompTox3.query("match == False")
                      .filter(["unidentified", "identified"])
                      )
fuzzyWrong = (pd.concat([fuzzyWrong, fuzzyCompTox3wrong], ignore_index=True)
              .drop_duplicates()
              )
# %%
"""Going to start fuzzy string matching with CosIng. The database had already
been imported up top. Time to actually use it now"""
unidentifiedAfterFuzzy3 = unidentifiedDF3.allCaps.drop_duplicates().tolist()
inci = CosIngDF.INCI.drop_duplicates().tolist()
fuzzyCosIng1 = fuzzy(unidentifiedAfterFuzzy3, inci, fuzz.partial_ratio, "partialRatio", 100, "similarity", fuzzyWrong)
# Holy fuck running this shit took 31 minutes
# %%
"""Let's export the fuzzy matching results so far. I'm gonna export an Excel
file that contains the fuzzy string matching results with CompTox, unidentified
names after fuzzy string matching with CompTox, and possible combinations of
unidentified ingredient names and INCI names from CosIng.
"""
fileName = "CompTox fuzzy.xlsx"
filePath = outputFolder/fileName

note = ["This file contains some results after fuzzy string matching with",
        "CompTox. I'm currently in the stage in which I am identifying",
        "ingredient names. I've already done case-insensitive strict matching",
        "between ingredient names and CompTox and CosIng. I'm currently doing",
        "fuzzy string matching between ingredient names that still have not",
        "been identified and known ingredients. This file contains results",
        "from fuzzy string matching with CompTox. The next step is to perform",
        "fuzzy string matching with CosIng.",
        "",
        "The tab 'Fuzzy CompTox' contains ingredients that have been",
        "identified using fuzzy string matching with CompTox, where the",
        "column 'unidentified' represent unidentified ingredient names from",
        "product ingredient lists while the column 'identified' contain",
        "ingredient names that have been identified from strict matching with",
        "CompTox. The previously unidentified names in this tab have now been",
        "matched to identified ingredients. Any names that are still",
        "unidentified are in the tab 'Unidentified'. The column 'ingredientName'",
        "lists the ingredient names as they originally appeared (after some",
        "cleaning on my part) on product ingredient lists, while the column",
        "'allCaps' contain the names after they've been rendered so that every",
        "letter is in uppercase. The uppercase names then went through fuzzy",
        "string matching with CosIng. This generated a preliminary list of",
        "36,672 possible combinations, which I stored in the tab 'Fuzzy CosIng'.",
        "I'll still need to go through this list to see which combinations",
        "are true matches.",
        "",
        "The tab 'Fuzzy wrong' contains combinations of unidentified ingredient",
        "names and identified ingredients names that are wrong matches. I'm",
        "keeping these combinations so that as I continue to do fuzzy string",
        "matching in the future, I can use this list to help narrow down",
        "possible combinations.",
        "",
        "As a final note, this spreadsheet only contains ingredients that",
        "have been identified after fuzzy string matching with CompTox. It",
        "does not contain ingredients that were identified using strict",
        "matching; these ingredients are stored elsewhere."]
readMe = pd.DataFrame({"Note": note})

if os.path.exists(fileName) is False:
    with pd.ExcelWriter(filePath) as w:
        readMe.to_excel(w, "ReadMe", index=False)
        fuzzyCompToxIdentified.to_excel(w, "Fuzzy CompTox", index=False)
        unidentifiedDF3.to_excel(w, "Unidentified", index=False)
        fuzzyCosIng1.to_excel(w, "Fuzzy CosIng", index=False)
        fuzzyWrong.to_excel(w, "Fuzzy wrong", index=False)
