# -*- coding: utf-8 -*-
"""
Created on Thu May 21 17:00:25 2026

@author: BChung
"""
from rapidfuzz import fuzz
import pandas as pd
import time


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
    start = time.time()
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
    end = time.time()
    duration = end - start
    seconds = duration % 60
    minutes = (duration - seconds)/60
    print("Generating combinations took {0:d} minutes and {1:.2f} seconds".format(minutes, seconds))
    if wrongMatches is not None:
        combinationsDF = (combinationsDF.merge(wrongMatches, "left", ["unidentified", "identified"], indicator=True)
                          .query("_merge == 'left_only'")
                          .drop(columns=["_merge"])
                          )
        print(combinationsDF.shape)
    combinationsDF = combinationsDF.sort_values([similarityName, "lengthsRatio", "unidentified"], ascending=[False, False, True], ignore_index=True)
    return combinationsDF
