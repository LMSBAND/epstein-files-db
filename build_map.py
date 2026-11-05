#!/usr/bin/env python3
"""Build the relationship map: clean person entities, score ties, write a standalone HTML map.

Usage: .venv/bin/python build_map.py [--nodes 180] [--min-shared 3] [--out epstein_files/output/map.html]

Why it differs from the old pyvis graph:
  * Names are cleaned and merged ("amanda kirby subject" -> "amanda kirby", "jeffrey"/"epstein" -> Epstein).
  * Epstein is left out of the drawing: he co-occurs with everyone, so his edges say nothing.
  * Ties are scored by association (shared / sqrt(files_a * files_b)), not raw co-occurrence, so
    prolific names don't tie to everything. Each person keeps only their strongest ties.
  * Clusters come from community detection, and every node links back to its documents.
"""
import argparse, json, math, re, sqlite3
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
import networkx as nx
from doc_dates import doc_date, era

ap = argparse.ArgumentParser()
ap.add_argument("--nodes", type=int, default=180)
ap.add_argument("--min-shared", type=int, default=3)
ap.add_argument("--top-ties", type=int, default=6)
ap.add_argument("--out", default="epstein_files/output/map.html")
a = ap.parse_args()

ALIASES = {
    "jeffrey epstein": "jeffrey epstein", "epstein": "jeffrey epstein", "jeffrey": "jeffrey epstein",
    "jeffrey e. epstein": "jeffrey epstein", "jeff epstein": "jeffrey epstein", "jeffery epstein": "jeffrey epstein",
    "jeffrey edward epstein": "jeffrey epstein", "mr. epstein": "jeffrey epstein", "jee": "jeffrey epstein",
    "ghislaine maxwell": "ghislaine maxwell", "ghislaine": "ghislaine maxwell", "maxwell": "ghislaine maxwell",
    "prince andrew": "prince andrew", "duke of york": "prince andrew",
    "bill clinton": "bill clinton", "william jefferson clinton": "bill clinton", "president clinton": "bill clinton",
    "donald trump": "donald trump", "donald j. trump": "donald trump", "president trump": "donald trump",
}
HUB = "jeffrey epstein"

# Public figures: always on the map, counted by name in the document text (NER misses many mentions).
# role = public role; group = filter bucket.
PINNED = {
    "donald trump":      (r"\bdonald (j\.? )?trump\b|\bpresident trump\b|\bmr\.? trump\b", "45th and 47th U.S. President", "Government"),
    "bill clinton":      (r"\b(bill|william (jefferson )?)clinton\b|\bpresident clinton\b", "42nd U.S. President", "Government"),
    "prince andrew":     (r"\bprince andrew\b|\bduke of york\b|\bandrew mountbatten", "British royal, former Duke of York", "Royalty"),
    "ghislaine maxwell": (r"\bghislaine\b", "Epstein associate; convicted of sex trafficking in 2021", "Associate"),
    "jean-luc brunel":   (r"\bjean[- ]luc brunel\b|\bbrunel\b", "Modeling agent; died in custody in 2022", "Associate"),
    "alan dershowitz":   (r"\bdershowitz\b", "Harvard Law professor emeritus; Epstein defense lawyer", "Legal"),
    "alex acosta":       (r"\b(alex|alexander) acosta\b", "Former U.S. Labor Secretary; U.S. Attorney on the 2008 plea deal", "Government"),
    "kathryn ruemmler":  (r"\bruemmler\b", "Former White House Counsel", "Government"),
    "larry summers":     (r"\b(larry|lawrence) summers\b", "Former U.S. Treasury Secretary and Harvard president", "Government"),
    "george mitchell":   (r"\bgeorge mitchell\b", "Former U.S. Senate Majority Leader", "Government"),
    "bill richardson":   (r"\bbill richardson\b", "Former New Mexico governor and U.N. ambassador", "Government"),
    "steve bannon":      (r"\b(steve|stephen) bannon\b", "Former White House chief strategist", "Government"),
    "howard lutnick":    (r"\blutnick\b", "Commerce Secretary in the second Trump administration", "Government"),
    "ehud barak":        (r"\behud barak\b", "Former Prime Minister of Israel", "Government"),
    "peter mandelson":   (r"\bmandelson\b", "British politician; former UK ambassador to the U.S.", "Government"),
    "les wexner":        (r"\b(les|leslie) wexner\b", "Founder of L Brands (Victoria's Secret)", "Business"),
    "leon black":        (r"\bleon black\b", "Co-founder of Apollo Global Management", "Business"),
    "jes staley":        (r"\b(jes|james) staley\b", "Former JPMorgan executive and Barclays CEO", "Business"),
    "glenn dubin":       (r"\bglenn dubin\b", "Hedge fund manager (Highbridge Capital)", "Business"),
    "bill gates":        (r"\bbill gates\b", "Microsoft co-founder", "Business"),
    "reid hoffman":      (r"\breid hoffman\b", "LinkedIn co-founder", "Business"),
    "sergey brin":       (r"\bsergey brin\b", "Google co-founder", "Business"),
    "elon musk":         (r"\belon musk\b", "Tesla and SpaceX CEO", "Business"),
    "noam chomsky":      (r"\bchomsky\b", "MIT linguist", "Academia"),
    "woody allen":       (r"\bwoody allen\b", "Filmmaker", "Entertainment"),
}
COUNTRY = {  # country where they hold or held their role
    "donald trump": "United States", "bill clinton": "United States", "prince andrew": "United Kingdom",
    "ghislaine maxwell": "United Kingdom / United States", "jean-luc brunel": "France",
    "alan dershowitz": "United States", "alex acosta": "United States", "kathryn ruemmler": "United States",
    "larry summers": "United States", "george mitchell": "United States", "bill richardson": "United States",
    "steve bannon": "United States", "howard lutnick": "United States", "ehud barak": "Israel",
    "peter mandelson": "United Kingdom", "les wexner": "United States", "leon black": "United States",
    "jes staley": "United States / United Kingdom", "glenn dubin": "United States", "bill gates": "United States",
    "reid hoffman": "United States", "sergey brin": "United States", "elon musk": "United States",
    "noam chomsky": "United States", "woody allen": "United States",
}
JUNK_WORDS = set("""subject sent from to cc bcc re fw fwd surcharge fedex bank beneficiary description schedule
partner transfer email suite lien seller failures llc inc ltd corp trust foundation account wire fax phone tel
mobile page date total amount invoice street avenue ave road floor court unknown redacted name attachment
attachments mail inbox outlook calendar meeting call office hotel airport payment balance statement
box kyc case client customer review file files report dear regards thanks doe
colour color depth pixel screen size space type pricing based notary public license licenses fraud securities
sender reopen overdraft trsfr islands goregaon taal glendoocr accetec glendower accetcc
businea checking enhanced obligations law
jan feb mar apr jun jul aug sep sept oct ocl nov dec dale discount cod delivered delvered subtotal detail period
charge charges handling remittance advice shipper reference airtime local successor trustee index stratum
package fuel debtor active signed number accciam equty buyout llp mellon bny pony civ material educational
background amicus
investors wireless cingular alpha express saver overnight pickup portfolio protection writing
release pram performance strong secret island norther miaow sts delivery grantor""".split())
SUFFIX = re.compile(r"(\s+(subject|sent|cc|to|from|wrote|re)\b.*$)|(/.*$)|(\s*<.*$)")


# Agent-researched categorization (epstein_files/output/categorize/batch_N.json): roles from document
# signatures, duplicate names, and entries that are not people.
CAT, DUP, NOT_PERSON = {}, {}, set()
for f in sorted(Path("epstein_files/output/categorize").glob("batch_[0-9].json")):
    for e in json.loads(f.read_text()):
        if e.get("kind") == "duplicate" and e.get("duplicate_of"):
            DUP[e["id"]] = e["duplicate_of"].lower()
        elif e.get("kind") == "not_person":
            NOT_PERSON.add(e["id"])
        else:
            CAT[e["id"]] = e
ALIASES.update({"leslie wexner": "les wexner", "liam o'sullivan": "liam osullivan",
                "stew stewart oldfield": "stewart oldfield", "bradle gillin": "bradley gillin",
                "brad gillin": "bradley gillin", "stew oldfield": "stewart oldfield",
                "paul morns": "paul morris",
                "abigail wenxer": "abigail wexner", "jeanie brennan": "jeanne brennan",
                "andersson dubin": "eva andersson dubin", "dave rodgers": "david rodgers",
                "tracy calvin dunlap": "tracy dunlap", "e. epstein": "jeffrey epstein",
                "carlo plajo-biroll": "carlo pirzio-biroli",
                "carlo pinio-biroll": "carlo pirzio-biroli", "darren lndyke": "darren indyke",
                "erika kellerhai": "erika kellerhals"})


def resolve(n):
    seen = set()
    while n in DUP and n not in seen:
        seen.add(n); n = DUP[n]
    return None if n in NOT_PERSON else ALIASES.get(n, n)


def clean(name):
    n = _clean(name)
    return resolve(n) if n else None


def _clean(name):
    n = name.lower().strip().strip("'\"`.,;:()[]-_ ")
    n = SUFFIX.sub("", n).strip("'\"`.,;:()[]-_ ")
    n = re.sub(r"\s+", " ", n)
    n = re.sub(r"['\u2019]s$", "", n)
    if n in ALIASES:
        return ALIASES[n]
    toks = n.split()
    if not (2 <= len(toks) <= 4) or len(n) > 40 or re.search(r"[0-9@#$%&*=+|\\]", n):
        return None
    if any(t in JUNK_WORDS for t in toks) or any(len(t.strip(".")) < 2 and not t.endswith(".") for t in toks):
        return None
    if not all(re.fullmatch(r"[a-z][a-z.'\-]*", t) for t in toks):
        return None
    if len(toks) == 3 and re.fullmatch(r"[a-z]\.?", toks[1]):      # drop middle initial
        n = f"{toks[0]} {toks[2]}"
    return ALIASES.get(n, n)


c = sqlite3.connect("file:epstein_files/epstein.db?mode=ro", uri=True, timeout=120)
fileinfo = {fid: (fn, ds) for fid, fn, ds in c.execute("SELECT id, filename, dataset FROM files")}
ALL_people_in = defaultdict(set)      # file_id -> people
mentions = Counter()
for fid, norm, cnt in c.execute("SELECT file_id, normalized, count FROM entities WHERE entity_label='PERSON'"):
    p = clean(norm)
    if p:
        ALL_people_in[fid].add(p); mentions[p] += cnt
pin_re = {p: re.compile(rx, re.I) for p, (rx, _, _) in PINNED.items()}
pin_any = re.compile("|".join(f"(?:{rx})" for rx, _, _ in PINNED.values()), re.I)
era_of = {}                            # file_id -> "pre" / "post" (relative to the June 2008 plea)
for fid, text in c.execute("SELECT file_id, extracted_text FROM text_cache WHERE char_count > 0"):
    e = era(doc_date(text))
    if e:
        era_of[fid] = e
    if not pin_any.search(text):
        continue
    for p, rx in pin_re.items():
        if rx.search(text):
            ALL_people_in[fid].add(p); mentions[p] += len(rx.findall(text))
era_count = {"pre": Counter(), "post": Counter()}   # person -> dated documents in each era
for f, ps in ALL_people_in.items():
    if (e := era_of.get(f)):
        for p in ps:
            era_count[e][p] += 1


def title(n):
    return " ".join(t.capitalize() if not re.fullmatch(r"[a-z]\.", t) else t.upper() for t in n.split())


def describe(n):
    if n in PINNED:
        return {"role": PINNED[n][1], "group": PINNED[n][2], "country": COUNTRY.get(n, ""), "evidence": [], "conf": "public"}
    e = CAT.get(n, {})
    role = ", ".join(x for x in (e.get("role", ""), e.get("org", "")) if x)
    return {"role": role, "group": e.get("group", "") if e.get("group") != "Unknown" else "",
            "country": e.get("country", ""), "evidence": e.get("evidence", [])[:3], "conf": e.get("confidence", "")}


def build(fids, label):
    """Graph data over the documents in fids."""
    people_in = {f: ALL_people_in[f] for f in fids if f in ALL_people_in}
    files_of = defaultdict(set)
    for f, ps in people_in.items():
        for p in ps:
            files_of[p].add(f)
    n_files = len(fids)
    hub_files = len(files_of.get(HUB, ()))

    ranked = [p for p, _ in sorted(((p, len(f)) for p, f in files_of.items() if p != HUB), key=lambda x: -x[1])]
    pinned = {p for p in PINNED if files_of.get(p)}
    keep = set(ranked[: a.nodes]) | pinned

    shared = Counter()
    for fid, ps in people_in.items():
        ps = sorted(ps & keep)
        if len(ps) > 60:                   # giant distribution lists say nothing about ties
            continue
        for x, y in combinations(ps, 2):
            shared[(x, y)] += 1

    ties = defaultdict(list)
    for (x, y), s in shared.items():
        if s < (1 if (x in pinned or y in pinned) else a.min_shared):
            continue
        w = s / math.sqrt(len(files_of[x]) * len(files_of[y]))
        ties[x].append((w, y, s)); ties[y].append((w, x, s))

    G = nx.Graph()
    for p in keep:
        for w, q, s in sorted(ties[p], reverse=True)[: a.top_ties]:
            if not G.has_edge(p, q):
                G.add_edge(p, q, w=w, s=s)
    G.remove_nodes_from([n for n in list(G) if G.degree(n) == 0])

    comms = sorted(nx.community.louvain_communities(G, weight="w", seed=7), key=len, reverse=True)
    comm_of = {n: i for i, cm in enumerate(comms) for n in cm}
    loners = sorted(pinned - set(G))          # public figures with no ties to anyone else on the map
    if loners:
        G.add_nodes_from(loners)
        comms.append(set(loners))
        for n in loners:
            comm_of[n] = len(comms) - 1

    def docs(p, k=12):
        fs = sorted(files_of[p], key=lambda f: -len(people_in[f] & keep))[:k]
        return [[fileinfo[f][0], fileinfo[f][1]] for f in fs if f in fileinfo]

    co = defaultdict(list)                 # person -> [(shared, other)] across everyone on the map
    for (x, y), s_ in shared.items():
        if x in G and y in G and s_ >= (1 if (x in pinned or y in pinned) else a.min_shared):
            co[x].append((s_, y)); co[y].append((s_, x))

    nodes = []
    for n in G:
        ds = Counter(fileinfo[f][1] for f in files_of[n] if f in fileinfo)
        nodes.append({"id": n, "name": title(n), "files": len(files_of[n]), "mentions": mentions[n],
                      "c": comm_of[n], "ds": dict(sorted(ds.items())), "docs": docs(n),
                      "withHub": len(files_of[n] & files_of.get(HUB, set())),
                      "co": [[o, s_] for s_, o in sorted(co[n], reverse=True)[:12]],
                      "pinned": n in pinned, "pre": era_count["pre"][n], "post": era_count["post"][n],
                      **describe(n)})
    links = [{"source": x, "target": y, "w": round(d["w"], 4), "s": d["s"]} for x, y, d in G.edges(data=True)]
    groups = []
    for i, cm in enumerate(comms):
        top = sorted(cm, key=lambda n: -len(files_of[n]))[:3]
        groups.append({"i": i, "size": len(cm), "label": ", ".join(title(t) for t in top),
                       "loners": bool(loners) and i == len(comms) - 1})
    print(f"[{label}] {len(nodes)} people, {len(links)} ties, {len(comms)} clusters over {n_files:,} files")
    print("  pinned:", ", ".join(f"{p} {len(files_of[p])}" for p in sorted(pinned, key=lambda p: -len(files_of[p]))))
    for g in groups[:5]:
        print(f"  cluster {g['i']} ({g['size']}): {g['label']}")
    return {"nodes": nodes, "links": links, "groups": groups,
            "meta": {"files": n_files, "hubFiles": hub_files, "people": len(files_of), "minShared": a.min_shared}}


all_f = set(fileinfo)
data = {"all": build(all_f, "all"),
        "pre": build({f for f in all_f if era_of.get(f) == "pre"}, "pre"),
        "post": build({f for f in all_f if era_of.get(f) == "post"}, "post"),
        "eras": {"total": len(all_f), "dated": len(era_of)}}
tpl = Path(__file__).with_name("map_template.html").read_text()
d3 = Path(__file__).with_name("lib").joinpath("d3.v7.min.js").read_text()
out = Path(a.out); out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(tpl.replace("/*D3*/", d3).replace("/*DATA*/null", json.dumps(data, separators=(",", ":"))))
print(f"-> {out}")
