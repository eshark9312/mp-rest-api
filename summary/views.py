from django.shortcuts import render
from django.http import HttpResponse
import pymongo, json
# from pymatgen.core import Structure
# from pymatgen.symmetry.analyzer import SpacegroupAnalyzer
# import crystal_toolkit.components as ctc
from .utils import extract_components, extract_components_with_wildcard, float_to_fraction, verified_elements
from .utils import get_formula_anonymous, generate_all_chemsyses_from_wildcard
from .utils import replace_nd_array


client = pymongo.MongoClient()
db = client.matproj_s3

# Create your views here.

def _pipeline(query_json:dict = {}, skip:int = 0, limit:int = 15, project_json:dict = {"_id": 0}, sort_list:list = []):
    collection = db['summary']
    pipeline =[{
        "$facet": {
            "data": [
                {"$match": query_json},
                {"$skip": skip},
                {"$limit": limit},
                {"$project": project_json}
            ],
            "meta" : [{"$match": query_json},
                    #   {"$match": {"tags": {"exists": False}}},
                      {"$count": "total_doc"}]
        }
    }]
    pipeline[0]["$facet"]["data"].extend([{"$sort": {sort[0]: sort[1]}} for sort in sort_list])
    paginated_response = list(collection.aggregate(pipeline))
    paginated_response[0]["meta"] = paginated_response[0]["meta"][0]
    return paginated_response[0]

def index(request):
    # query builder
    query_json = {}
    print("===========Query_params=================")
    for key, value in request.GET.items():
        print(f"{key}: {value}")
    print("=========== End =================")

    if "chemsys" in request.GET:
        elements = request.GET["chemsys"].split('-')
        if '*' in elements:
            # if wildcard is used for search, then generate all the possible combination of chemsyses
            all_chemsyses = generate_all_chemsyses_from_wildcard(elements)
            if "$and" in query_json:
                query_json["$and"].append({"$or": all_chemsyses})
            else:
                query_json["$and"] = [{"$or": all_chemsyses}]
        else:
            query_json["chemsys"] = "-".join(sorted(elements))

    if "elements" in request.GET:
        elements = verified_elements(request.GET["elements"].split(','))
        if "elements" in query_json:
            query_json["elements"].update({"$all": elements})
        else:    
            query_json["elements"] = {"$all": elements}

    if "exclude_elements" in request.GET:
        exclude_elements = verified_elements(request.GET["exclude_elements"].split(','))
        if "elements" in query_json:
            query_json["elements"].update({"$nin": exclude_elements})
        else:    
            query_json["elements"] = {"$nin": exclude_elements}

    if "formula" in request.GET:
        formula = request.GET["formula"]
        # if chemical formula is used, then extract formula_anonymous and chemsys, then run search 
        if '*' in request.GET["formula"]:
            # if wildcard is used in formula, then generate all chemsyses and formula_anonymous
            normal_components = extract_components(formula)
            wildcard_components = extract_components_with_wildcard(formula)
            elements = list(normal_components.keys())
            elements.extend(["*" for wildcard in wildcard_components])
            print(">>> elements", elements)
            all_chemsyses = generate_all_chemsyses_from_wildcard(elements)
            if "$and" in query_json:
                query_json["$and"].append({"$or": all_chemsyses})
            else:
                query_json["$and"] = [{"$or": all_chemsyses}]
            # get formula_anonymous
            normal_components.update(wildcard_components)
            query_json["formula_anonymous"] = get_formula_anonymous(normal_components)
        else:
            components = extract_components(formula)
            query_json["formula_anonymous"] = get_formula_anonymous(components)
            query_json["chemsys"] = "-".join(sorted(list(components.keys())))

    if "material_ids" in request.GET:
        material_ids = request.GET["material_ids"]
        mp_ids = [mp_id.strip() for mp_id in material_ids.split(',')]
        query_json["material_id"] = {"$in":mp_ids}

    if "nelements_min" in request.GET or "nelements_max" in request.GET:
        nelements_query = {}
        if "nelements_min" in request.GET:
            nelements_query["$gte"] = int(request.GET["nelements_min"])
        if "nelements_max" in request.GET:
            nelements_query["$lte"] = int(request.GET["nelements_max"])
        query_json["nelements"] = nelements_query


    print("===========Query_JSON=================")
    print(query_json)
    print("=========== End =================")
    # get options for the query
    required_fields = request.GET['_fields'].split(',')
    required_fields_json = {"_id": 0}
    for required_field in required_fields:
        required_fields_json[required_field] = 1
    sort_fields_list = []
    if '_sort_fields' in request.GET:
        sort_fields = request.GET['_sort_fields'].split(',')
        for sort_field in sort_fields:
            if sort_field.startswith('-'):
                sort_fields_list.append((sort_field[1:], -1))
            else:
                sort_fields_list.append((sort_field, 1))
    limit_docs = int(request.GET['_limit'])
    skip_docs = int(request.GET['_skip'])
    # run query and return response
    response = _pipeline(query_json,skip_docs,limit_docs,required_fields_json,sort_fields_list)
    print("===========Response=================")
    print(response)
    print("=========== End =================")
    return HttpResponse(json.dumps(response))


# def detail(request, materialID_num):
#     print(request)
#     print("---------")
#     print(materialID_num)
#     materialID = f"mp-{materialID_num}"
#     # get summary data
#     collection = db['summary']
#     project = {"_id": 0, 
#                "formula_pretty": 1, 
#                "energy_above_hull": 1,
#                "symmetry.symbol": 1,
#                "band_gap": 1,
#                "formation_energy_per_atom": 1,
#                "ordering": 1,
#                "total_magnetization": 1,
#                "theoretical": 1,
#                "structure": 1,
#                }
#     result = collection.find_one({"material_id": materialID}, project)
#     magnetic_ordering = {
#         'NM': 'Non-magnetic', 
#         'FM': 'Ferro-magnetic', 
#         'FiM': 'Ferrimagnetic'
#     }
#     summary_data = {
#       'Energy Above Hull': f"{result['energy_above_hull']:.3f} eV/atom",
#       'Space Group': f"{result['symmetry']['symbol']}",
#       'Band Gap': f"{result['band_gap']:.2f} eV",
#       'Predicted Formation Energy': f"{result['formation_energy_per_atom']:.3f} eV/atom",
#       'Magnetic Ordering': magnetic_ordering[result['ordering']],
#       'Total Magnetization': f"{result['total_magnetization']:.2f} µB/f.u.",
#       'Experimentally Observed': 'No' if result['theoretical'] else 'Yes',
#     }
#     lattice = result['structure']['lattice']
#     lattice_data ={
#         'a': f"{lattice['a']:.2f} Å",
#         'b': f"{lattice['b']:.2f} Å",
#         'c': f"{lattice['c']:.2f} Å",
#         'α': f"{lattice['alpha']:.2f} º",
#         'β': f"{lattice['beta']:.2f} º",
#         'ɣ': f"{lattice['gamma']:.2f} º",
#     }
#     # get CrystalToolkit Scene & legend data
#     pymat_structure = Structure.from_dict(result['structure'])
#     structure_component = ctc.StructureMoleculeComponent(pymat_structure, id="my_structure")
#     scene_data = structure_component.initial_data['scene']
#     legend_data = structure_component.initial_data['legend_data']
#     replace_nd_array(scene_data)
#     # get description data
#     description = db['robocrys'].find({"material_id": materialID}, {"_id": 0, "description": 1}).next()
#     # get symmetry data
#     sg_analyzer = SpacegroupAnalyzer(pymat_structure)
#     sym_dataset = sg_analyzer.get_symmetry_dataset()
#     sym_data = {
#         "Crystal System": sg_analyzer.get_crystal_system().capitalize(),
#         "Lattice System": sg_analyzer.get_lattice_type().capitalize(),
#         "Hall Number": sym_dataset['hall'],
#         "International Number": sym_dataset['number'],
#         "Symbol": sym_dataset['international'],
#         "Point Group": sym_dataset['pointgroup'],
#     }
#     # get Wyckoff sites
#     sym_struct = sg_analyzer.get_symmetrized_structure()
#     wyckoff_data = sorted(
#                           zip(sym_struct.wyckoff_symbols, sym_struct.equivalent_sites),
#                           key=lambda x: "".join(filter(str.isalpha, x[0])),
#                          )
#     wyckoff_sites = []
#     for sym_sites in wyckoff_data:
#         site_symbol = sym_sites[0]
#         wyckoff_site = sym_sites[1][0]
#         wyckoff_sites.append({
#                 "Wyckoff": site_symbol,
#                 "Element": f"{wyckoff_site.specie}",
#                 "x": float_to_fraction(wyckoff_site.a),
#                 "y": float_to_fraction(wyckoff_site.b),
#                 "z": float_to_fraction(wyckoff_site.c),
#             })
#     # response with data
#     response = {
#         "summary_data": summary_data,
#         "lattice_data": lattice_data,
#         "scene_data": scene_data,
#         "legend_data": legend_data,
#         "description": description,
#         "sym_data": sym_data,
#         "wyckoff_sites": wyckoff_sites,
#     }
#     return HttpResponse(json.dumps({"data": response}))

def detail_dash(requst, materialID_str):
    collection = db['summary']
    result = collection.find_one({"material_id": materialID_str}, {"_id": 0, "structure": 1})
    return HttpResponse(json.dumps(result))
