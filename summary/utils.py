import re, itertools
from typing import Dict, Union
from numpy import ndarray
from fractions import Fraction

def generate_all_chemsyses_from_wildcard(elements):
    """
    generate all chemsyses from wildcard and return regex operators for them
    
    Args:
        elements list(str): chemical elements and wildcards
        
    Returns:
        list(dict): A dictionary containing the generated regex chemsyses

    Example usage:
        chemical_formula = "Li-Fe-*"
        result = generate_all_chemsyses_from_wildcard(chemical_formula)
        print(result)
        >> [
                {"chemsys": {"$regex": "Fe-Li-.*$"},
                {"chemsys": {"$regex": "Fe-.*-Li$"},
                {"chemsys": {"$regex": "^.*-Fe-Li$"}
           ]
    """
    all_chemsyses = []
    for permute in itertools.permutations(elements, len(elements)):
        # check if permute is sorted one
        list_elements = list(permute)
        while '*' in list_elements:
            list_elements.remove('*')
        list_elements_sorted = sorted(list_elements)
        if list_elements_sorted == list_elements and permute not in all_chemsyses:
            wildcard_pattern = "-".join(permute)
            regex_pattern = '^' + re.sub(r'\*', r'.*', wildcard_pattern) + '$'
            all_chemsyses.append({"chemsys": {"$regex": regex_pattern}})
    return all_chemsyses

def extract_components(chemical_formula):
    """
    Extracts the components from a chemical formula.
    
    Args:
        chemical_formula (str): The chemical formula to be processed.
        
    Returns:
        dict: A dictionary containing the extracted components and their counts.

    Example usage:
        chemical_formula = "Eu2SiCl2O3"
        result = extract_components(chemical_formula)
        print(result)
        >> {'Eu': 2, 'Si': 1, 'Cl': 2, 'O': 3}
    """
    pattern = r"([A-Z][a-z]*)(\d*)"
    components = {}
    
    for match in re.finditer(pattern, chemical_formula):
        element = match.group(1)
        count = int(match.group(2)) if match.group(2) else 1
        components[element] = components.get(element, 0) + count
    
    return components

def extract_components_with_wildcard(chemical_formula):
    pattern = r"(\*)(\d*)"
    components = {}
    wildcard_num = 0
    for match in re.finditer(pattern, chemical_formula):
        element = match.group(1) + str(wildcard_num)
        count = int(match.group(2)) if match.group(2) else 1
        components[element] = components.get(element, 0) + count
        wildcard_num += 1
    return components

def get_formula_anonymous(components):
    """
    Extracts the formula_anonymous and chemsys from a list of components

    Args:
        components list(dict): A dictionary containing 
        
    Returns:
        dict: formula_anonymous

    Example usage:
        chemical_formula = "Eu2SiCl2O3"
        components = extract_components(chemical_formula)
        result = get_formula_anonymous(components)
        print(result)
        >> AB2C2D3
    """
    formula_anonymous = ""
    elements_sorted_by_count = sorted(components.items(), key=lambda x: x[1], reverse=False)
    rep_element = 'A'
    for element, count in elements_sorted_by_count:
        formula_anonymous += rep_element + (str(count) if count > 1 else '')
        rep_element = chr(ord(rep_element) + 1)
    return formula_anonymous

def verified_elements(elements):
    stripped_elements = [element.strip() for element in elements]
    while '' in stripped_elements:
        stripped_elements.remove('')
    return stripped_elements

def replace_nd_array(iterable):
    if type(iterable) is dict:
        for key, value in iterable.items():
            if type(value) is ndarray:
                iterable[key] = value.tolist()
            if type(value) is dict or type(value) is list:
                replace_nd_array(value)
    elif type(iterable) is list:
        for i in range(len(iterable)):
            if type(iterable[i]) is ndarray:
                iterable[i] = iterable[i].tolist()
            if type(iterable[i]) is dict or type(iterable[i]) is list:
                replace_nd_array(iterable[i])

def float_to_fraction(float_value, tolerance = 1e-4, limit_denominator = 2**6):
    """
    Converts a float value to a fraction.
    
    Args:
        float_value (float): The float value to be converted.
        tolerance (float): The tolerance used for rounding the fraction.
        limit_denominator (int): The limit for denominator.
    Returns:
        str: The float value represented as a fraction.
    """
    try:
        fraction = Fraction(float_value).limit_denominator(20)
        if abs(float_value - float(fraction)) < tolerance:
            if fraction.numerator == 0:
                return "0"
            else:
                return f"{fraction.numerator}/{fraction.denominator}"
        else:
            return f"{float_value:.4f}"
    except (ValueError, OverflowError):
        return f"{float_value:.4f}"