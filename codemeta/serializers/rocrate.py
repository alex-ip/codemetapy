import sys
import json
import os.path
from pprint import pformat
from typing import Union, IO, Sequence, Optional
from rdflib import Graph, URIRef, BNode, Literal
from rdflib.namespace import SKOS #type: ignore
from copy import copy
from codemeta.common import (
    AttribDict,
    CODEMETA_SOURCE,
    CODEMETA_LOCAL_SOURCE,
    SCHEMA_SOURCE,
    SCHEMA_LOCAL_SOURCE,
    STYPE_SOURCE,
    STYPE_LOCAL_SOURCE,
    IODATA_SOURCE,
    IODATA_LOCAL_SOURCE,
    init_context,
    REPOSTATUS_LOCAL_SOURCE,
    REPOSTATUS_SOURCE,
    PREFER_URIREF_PROPERTIES,
    PREFER_LITERAL_PROPERTIES,
    TMPDIR,
    DEVIANT_CONTEXT,
    ORDEREDLIST_PROPERTIES,
)

import codemeta.serializers.jsonld as jsonld


# def hide_ordered_lists(
#     data: Union[dict, list, tuple, str], key: Optional[str] = None
# ) -> Union[dict, list, str]:
#     """Hide explicit @list nodes on known ordered list properties, on read-in they will be assumed agained via the (manipulated) context"""
#     if isinstance(data, dict):
#         if "@list" in data and key in jsonld.ORDEREDLIST_PROPERTIES_NAMES:
#             return hide_ordered_lists(data["@list"])
#         else:
#             for k, v in data.items():
#                 data[k] = hide_ordered_lists(v, k)
#     elif isinstance(data, (list, tuple)):
#         return [hide_ordered_lists(v) for v in data]
#     return data


def embed_items(data, itemmap: dict, history: set):
    """SELECTIVELY replace all references with items, auxiliary function for object framing. The history prevents circular references."""
    # if isinstance(data, list):
    #     for i, item in enumerate(data):
    #         print(f"DEBUG processing list, #history: {len(history)}", file=sys.stderr)
    #         data[i] = embed_items(item, itemmap, copy(history))  # recursion step
    if isinstance(data, dict):
        for idkey in ("@id", "id"):
            if idkey in data and data[idkey] in itemmap and data[idkey] not in history:
                print(f"DEBUG embedded {data[idkey]} (explicit)", file=sys.stderr)
                history.add(data[idkey])
                print(f"DEBUG (recursing over embedded content)", file=sys.stderr)
                if "_embedding_done" in itemmap[data[idkey]]:
                    data = itemmap[data[idkey]]
                else:
                    data = embed_items(itemmap[data[idkey]], itemmap, copy(history))
                    data["_embedding_done"] = True
                return data
            # elif idkey in data and data[idkey] not in itemmap:
            #    print(f"DEBUG could not embed {data[idkey]}, not in graph", file=sys.stderr)
            # elif idkey in data and data[idkey] and data[idkey] in history:
            #    print(f"DEBUG could not embed {data[idkey]}, already in history, returning a reference", file=sys.stderr)
            #    return { idkey: data[idkey] }
        for k, v in data.items():
            if (k not in ("@id", "id")
                and k not in jsonld.NOEMBED
                and all( str(x) != k and str(x).split('/')[-1] != k for x in PREFER_LITERAL_PROPERTIES )
                and k.startswith('file:///stub/')  # Only embed stubs
                ):
                print(f"DEBUG processing key {k}, #history: {len(history)}", file=sys.stderr)
                data[k] = embed_items(v, itemmap, copy(history))  # recursion step
    elif (
        isinstance(data, str)
        and (
            data.startswith(("http", "file://", "/", "_"))
            or data.startswith(jsonld.NSPREFIXES)
        )
        and data in itemmap
        and data not in history
    ):  # this is probably a reference even though it's not explicit
        # data is an URI reference we can resolve
        history.add(data)
        print(f"DEBUG embedded {data} (implicit), recursing over embedded content", file=sys.stderr)
        if "_embedding_done" in itemmap[data]:
            data = itemmap[data]
        else:
            data = embed_items(itemmap[data], itemmap, copy(history))
            data["_embedding_done"] = True
    return data


def expand_stubs(data, itemmap: dict, history: set):
    """SELECTIVELY replace all references with items, auxiliary function for object framing. The history prevents circular references."""
    # if isinstance(data, list):
    #     for i, item in enumerate(data):
    #         print(f"DEBUG processing list, #history: {len(history)}", file=sys.stderr)
    #         data[i] = expand_stubs(item, itemmap, copy(history))  # recursion step
    if isinstance(data, dict):
        for idkey in ("@id", "id"):
            if idkey in data and data[idkey] in itemmap and data[idkey] not in history:
                print(f"DEBUG embedded {data[idkey]} (explicit)", file=sys.stderr)
                history.add(data[idkey])
                print(f"DEBUG (recursing over embedded content)", file=sys.stderr)
                if "_embedding_done" in itemmap[data[idkey]]:
                    data = itemmap[data[idkey]]
                else:
                    data = expand_stubs(itemmap[data[idkey]], itemmap, copy(history))
                    data["_embedding_done"] = True
                print("DEBUG data", pformat(data), file=sys.stderr)
                return data
            # elif idkey in data and data[idkey] not in itemmap:
            #    print(f"DEBUG could not embed {data[idkey]}, not in graph", file=sys.stderr)
            # elif idkey in data and data[idkey] and data[idkey] in history:
            #    print(f"DEBUG could not embed {data[idkey]}, already in history, returning a reference", file=sys.stderr)
            #    return { idkey: data[idkey] }
        for k, v in data.items():
            if (k not in ("@id", "id")
                and k not in jsonld.NOEMBED
                and all( str(x) != k and str(x).split('/')[-1] != k for x in PREFER_LITERAL_PROPERTIES )
                ):
                print(f"DEBUG processing key {k}, #history: {len(history)}", file=sys.stderr)
                data[k] = expand_stubs(v, itemmap, copy(history))  # recursion step

    elif (
        isinstance(data, str)
        and (
            # data.startswith(('file:///stub/', "_"))
            data.startswith(("http", "file://", "/", "_"))
            or data.startswith(jsonld.NSPREFIXES)
        )
        and data in itemmap
        and data not in history
    ):  # this is probably a reference even though it's not explicit
        # data is an URI reference we can resolve
        history.add(data)
        print(f"DEBUG embedded {data} (implicit), recursing over embedded content", file=sys.stderr)
        if "_embedding_done" in itemmap[data] and not data.startswith(('file:///stub/', "_")):
            print('blah', file=sys.stderr)
            data = itemmap[data]
        else:
            data = expand_stubs(itemmap[data], itemmap, copy(history))
            data["_embedding_done"] = True
    print("DEBUG data", pformat(data), file=sys.stderr)
    return data


def do_object_framing(
    data: dict, res_id: str, history: set = set(), preserve_context: bool = True
):
    """JSON-LD object framing. Rdflib's json-ld serialiser doesn't implement this so we do this ourselves"""
    itemmap = {}  # mapping from ids to python dicts
    if "@graph" in data:
        jsonld.gather_items(data["@graph"], itemmap)
    else:
        jsonld.gather_items(data, itemmap)

    print("DEBUG res_id", res_id, file=sys.stderr)
    print("DEBUG data", pformat(data), file=sys.stderr)
    print("DEBUG itemmap", pformat(itemmap), file=sys.stderr)

    if res_id not in itemmap:
        raise Exception(f"Resource {res_id} not found in tree, framing not possible")

    for item in itemmap.values():
        expand_stubs(item, itemmap, history)

    # if "@context" in data and preserve_context:
    #     # preserve context
    #     itemmap[res_id]["@context"] = data["@context"]

    data["@graph"] = list(itemmap.values())
    return data


def serialize_to_rocrate(
    g: Graph, res: Union[Sequence, URIRef, None], args: AttribDict
) -> dict:
    """Serializes the RDF graph to flat JSON-LD for RO-crate"""

    #                                              v--- the internal 'deviant' context is required for the serialisation to work, it will be stripped later in rewrite_context()
    context = [x[0] for x in init_context(args)] + [DEVIANT_CONTEXT]
    data = json.loads(g.serialize(format="json-ld", auto_compact=True, context=context))
    print("DEBUG data (after serialize)", pformat(data), file=sys.stderr)

    # rdflib doesn't do 'object framing' so we have to do it in this post-processing step
    # if we have a single resource, it'll be the focus object the whole frame will be built around
    if res and (not isinstance(res, (list, tuple)) or len(res) == 1):
        assert isinstance(res, URIRef)
        if args.includecontext:
            data = jsonld.expand_implicit_id_nodes(
                data, [str(x).split("/")[-1] for x in PREFER_URIREF_PROPERTIES]
            )
        print("DEBUG data (after expand_implicit_id_nodes)", pformat(data), file=sys.stderr)


        # data = jsonld.do_object_framing(data, str(res))
        data = do_object_framing(data, str(res))
        print("DEBUG data (after do_object_framing)", pformat(data), file=sys.stderr)


        # Hide explicit @list nodes, on read-in they will be assumed agained via the (manipulated) context
        data = jsonld.hide_ordered_lists(data)
        print("DEBUG data (after hide_ordered_lists)", pformat(data), file=sys.stderr)
        assert isinstance(data, dict)

        root, parent = jsonld.find_main(data, res)
        if parent and len(data["@graph"]) == 1 and res:
            # No need for @graph if it contains only one item now:
            assert isinstance(root, dict)
            parent.update(root)
            del data["@graph"]
            root = parent

        data = jsonld.sort_by_position(data)
        print("DEBUG data (after sort_by_position)", pformat(data), file=sys.stderr)
    else:
        # we have a graph of multiple resources, structure is mostly stand-off: we do object framing on each SoftwareSourceCode instance (this does lead to some redundancy)
        if args.includecontext:
            data = jsonld.expand_implicit_id_nodes(
                data, [str(x).split("/")[-1] for x in PREFER_URIREF_PROPERTIES]
            )
        if "@graph" in data:
            new_graph = []
            for item in data["@graph"]:
                if (
                    isinstance(item, dict)
                    and item.get("@type", item.get("type", None))
                    == "SoftwareSourceCode"
                ):
                    item_id = item.get("@id", item.get("id", None))
                    if item_id:
                        new_graph.append(
                            jsonld.do_object_framing(data, item_id, preserve_context=False)
                        )
            data["@graph"] = new_graph
        data = jsonld.hide_ordered_lists(data)
        data = jsonld.sort_by_position(data)

    assert isinstance(data, dict)
    if "@context" in data:
        # remap local context references to URLs
        data["@context"] = jsonld.rewrite_context(data["@context"], args.addcontext)

    # we may have some lingering prefixes which we don't need and we want @id and @type instead of 'id' and 'type', cleanup:
    data = jsonld.cleanup(data, args.baseuri)

    assert isinstance(data, dict)
    return data
