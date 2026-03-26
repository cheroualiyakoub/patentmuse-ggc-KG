# kb_builder/npl_parser.py

import re
import logging
from typing import List, Dict, Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load NLTK English words once at import time (cached as frozenset for speed)
# ---------------------------------------------------------------------------
try:
    from nltk.corpus import words as _nltk_words_corpus
    _ENGLISH_WORDS = frozenset(w.lower() for w in _nltk_words_corpus.words() if 3 <= len(w) <= 15)
except LookupError:
    import nltk
    nltk.download('words', quiet=True)
    from nltk.corpus import words as _nltk_words_corpus
    _ENGLISH_WORDS = frozenset(w.lower() for w in _nltk_words_corpus.words() if 3 <= len(w) <= 15)
except ImportError:
    _ENGLISH_WORDS = frozenset()
    log.warning("NLTK not installed — author name filtering reduced. Run: pip install nltk")

# Common English words that are ALSO valid surnames — never reject these
_SURNAME_EXCEPTIONS = frozenset({
    'young', 'brown', 'green', 'black', 'white', 'gray', 'grey', 'gold',
    'king', 'knight', 'hunter', 'fisher', 'baker', 'cook', 'chase', 'cross',
    'bell', 'hill', 'ford', 'wells', 'stone', 'burns', 'ross', 'bond',
    'hope', 'sharp', 'wise', 'frost', 'bloom', 'woods', 'bush', 'vance',
    'short', 'long', 'little', 'love', 'best', 'good', 'fair', 'swift',
    'rich', 'noble', 'frank', 'grace', 'page', 'english', 'french',
    # Very common surnames that are also dictionary words
    'smith', 'lee', 'wolf', 'fox', 'lamb', 'crane', 'sparrow', 'hawk',
    'wolf', 'lyon', 'lyon', 'mann', 'chan', 'yang', 'park', 'kim',
})

# Gerunds and verb forms that never appear as surnames
# (NLTK only has base forms so gerunds slip through the NLTK check)
_VERB_FORMS = frozenset({
    'capturing', 'sequencing', 'detecting', 'targeting', 'mapping',
    'using', 'combining', 'profiling', 'screening', 'analyzing',
    'identifying', 'characterizing', 'measuring', 'monitoring',
    'selecting', 'isolating', 'amplifying', 'quantifying', 'encoding',
    'processing', 'computing', 'generating', 'predicting', 'introducing',
    'comparing', 'evaluating', 'estimating', 'determining', 'applying',
    'assessing', 'implementing', 'developing', 'presenting', 'describing',
    'improving', 'optimizing', 'integrating', 'validating', 'classifying',
})

# Words that slip through NLTK (plurals, abbreviations, compound terms)
# but are clearly not surnames — curated from real pipeline false positives
_CURATED_FALSE_POSITIVES = frozenset({
    # Medical/scientific nouns (NLTK misses compound or uncommon terms)
    'ultrasound', 'seminars', 'imaging', 'therapy', 'surgery',
    # Abbreviations that match the initials+lastname pattern
    'dept', 'civ', 'engl', 'proc', 'natl', 'assoc', 'univ',
    'hosp', 'inst', 'intl', 'corp', 'govt', 'comm', 'acad',
    # Lab/journal abbreviations not in NLTK corpus (informal/abbreviated)
    'lab', 'labs', 'res', 'rev', 'lett', 'bull', 'ann',
    # Journal name fragments
    'med', 'sci', 'biol', 'chem', 'phys', 'tech', 'eng',
    # Drug/compound names seen in logs (e.g. "Markham A. Lurbinectedin: A Review...")
    'lurbinectedin',
})

class NPLParser:
    """
    Robust NPL parser that ONLY extracts authors from genuine academic papers.
    Filters out:
    - Product listings (Amazon, Alibaba, etc.)
    - Office actions
    - Standards documents
    - URLs and website citations
    - Design patents
    """
    
    # ============================================================================
    # BLACKLIST: Skip these entirely
    # ============================================================================
    
    # Skip if ANY of these appear in the citation
    SKIP_KEYWORDS = {
        # Product/Shopping sites
        'amazon.com', 'amazon.ca', 'alibaba.com', 'ebay.com', '9to5toys',
        'bhphotovideo.com', 'b&h',
        
        # Patent citations and procedural documents
        'u.s. pat', 'us pat', 'u.s.pat', 'patent no.', 'pat. no.',
        'file history', 'file wrapper', 'prosecution history',
        'patent owner', 'patentee', 'assignee:', 'issued to',
        'excerpts from', 'ipr2', 'ptab', 'inter partes',
        'exhibit 10', 'case no.', 'petition for',
        
        # Office actions and legal docs
        'office action', 'first office', 'final office', 'office rejection',
        'examination report', 'search report', 'international search',
        'priority document',
        
        # Standards/Specifications
        'ieee std', 'iso/iec', 'rfc ', 'ansi ', 'itu-t', 'technical committee',
        'forum technical', 'working group', 'specification',
        
        # Design patents
        'design no.', 'cnipa', 'wipo.int/designdb',
        
        # Generic web content
        'site visited', 'visited on', 'retrieved from', 'available at',
        'first available', 'published by', 'suitcase', 'luggage',
        
        # Product-specific
        'hard shell', 'carry-on', 'spinner', 'trolley', 'backpack',
    }
    
    # Skip if citation starts with these
    SKIP_PREFIXES = [
        'http://', 'https://', 'www.',
        'amazon', 'alibaba', 'ebay',
        'design no', 'patent no',
    ]
    
    # Skip if citation matches these patterns
    SKIP_PATTERNS = [
        # Patent citations and procedural documents
        r'\bu\.?s\.?\s*pat',  # U.S. Pat., US Pat, U.S.Pat
        r'\bpatent\s+(no|owner|history)',  # Patent No., Patent Owner, Patent History
        r'\bfile\s+(history|wrapper)',  # File History, File Wrapper
        r'\bipr\s*\d{4}',  # IPR2024-00035
        r'\bptab\s+case',  # PTAB Case
        r'\bexhibit\s+\d{4}',  # Exhibit 1021
        r'\b(us|ep|wo|jp|cn)-?\d{7,}',  # Patent numbers (7+ digits)
        r'\bpat\.\s*no\.\s*\d',  # Pat. No. 123456
        
        # Shopping sites
        r'\b(amazon|alibaba|ebay)\.(com|ca|co\.uk)',
        
        # Office actions
        r'\boffice\s+action\b',
        r'\b(app|appl?|application)\.\s*no',
        
        # Patent family references
        r'\b(pct|wo|ep|us|cn|jp)/\d{4}',
        r'\bdesign\s+no\.',
        
        # Website visits
        r'\bfirst\s+available\b',
        r'\bsite\s+visited\b',
        r'\bhttps?://',
    ]
    
    # Given names that appear solo in "Givenname et al." patterns
    # when citation puts given name before family name (common in East Asian
    # and some European citation formats). Seen in real pipeline logs.
    KNOWN_GIVEN_NAMES = {
        # Chinese given names (seen in logs: Tiesheng, Xiangwei, Tailu, etc.)
        'tiesheng', 'xiangwei', 'tailu', 'renguang', 'tianshi', 'yansong',
        'fengyuan', 'qingguang', 'wei', 'jun', 'fang', 'hui', 'ling',
        'ming', 'hong', 'yan', 'ping', 'lei', 'tao', 'gang', 'peng',
        'jian', 'feng', 'qiang', 'rong', 'hao', 'xin', 'yong', 'jing',
        'bin', 'zhi', 'chao', 'kun', 'liang', 'qing', 'hua', 'sheng',
        'dong', 'nan', 'tasunori', 'kenji', 'hiroshi', 'takashi', 'kazuo',
        # Japanese given names (seen in logs)
        'akinori', 'yuzuru', 'masako', 'naoki', 'ryuichi', 'katsuhiro',
        'katsukiyo', 'tetsuro', 'nobuyuki', 'yoshihiro', 'masahiro',
        'tomohiro', 'shinichi', 'koichi', 'tsuyoshi', 'daisuke', 'yasuhiro',
        # European given names (seen in logs: Arno, Bernhard, Audun)
        'arno', 'bernhard', 'audun', 'bjorn', 'soren', 'gunnar', 'leif',
        'sigrid', 'ingrid', 'astrid', 'dag', 'arne', 'olaf', 'torben',
        # Italian/Southern European given names (seen in logs)
        'elena', 'enrico', 'simona', 'luigi', 'marco', 'paolo', 'luca',
        'gianluca', 'roberto', 'stefano', 'antonio', 'giovanni', 'andrea',
        # Indian subcontinent given names (seen in logs)
        'nandini', 'priya', 'deepa', 'kavita', 'anita', 'sunita', 'rekha',
        'suresh', 'rajesh', 'ramesh', 'dinesh', 'ganesh', 'mahesh',
    }
    # Keep old name as alias for backwards compat
    ASIAN_GIVEN_NAMES = KNOWN_GIVEN_NAMES

    # Words that indicate product names, not authors
    PRODUCT_WORDS = {
        'suitcase', 'luggage', 'bag', 'case', 'box', 'trolley',
        'spinner', 'hardside', 'shell', 'pack', 'trunk', 'organizer',
        'protector', 'carry-on', 'travel', 'elite', 'premium',
        'tote', 'tackle', 'fishing', 'storage', 'divider', 'plastic',
        'durable', 'waterproof', 'removable', 'portable', 'bamboo',
        'samsonite', 'pelican', 'rimowa', 'nanuk', 'yeti',
    }
    
    # ============================================================================
    # VALIDATION
    # ============================================================================
    
    @staticmethod
    def should_skip_citation(npl_text: str) -> bool:
        """
        Determine if this citation should be skipped entirely.
        Returns True if it's not an academic paper.
        """
        if not npl_text or len(npl_text) < 20:
            return True
        
        text_lower = npl_text.lower()
        
        # Skip if contains any blacklisted keywords
        for keyword in NPLParser.SKIP_KEYWORDS:
            if keyword in text_lower:
                log.debug(f"⛔ Skipping (keyword '{keyword}'): {npl_text[:60]}...")
                return True
        
        # Skip if starts with blacklisted prefix
        for prefix in NPLParser.SKIP_PREFIXES:
            if text_lower.startswith(prefix):
                log.debug(f"⛔ Skipping (prefix '{prefix}'): {npl_text[:60]}...")
                return True
        
        # Skip if matches blacklisted patterns
        for pattern in NPLParser.SKIP_PATTERNS:
            if re.search(pattern, npl_text, re.IGNORECASE):
                log.debug(f"⛔ Skipping (pattern match): {npl_text[:60]}...")
                return True
        
        return False
    
    @staticmethod
    def is_likely_academic(npl_text: str) -> bool:
        """
        Check if citation has academic paper indicators.
        """
        text_lower = npl_text.lower()
        
        # Positive indicators for academic papers
        academic_indicators = [
            'et al',  # Multiple authors
            'journal', 'proceedings', 'conference',
            'vol.', 'volume', 'pp.', 'pages',
            'doi:', 'arxiv',
            'ieee', 'acm', 'springer', 'elsevier',
            'nature', 'science', 'pnas',
        ]
        
        # Must have at least ONE academic indicator
        has_indicator = any(ind in text_lower for ind in academic_indicators)
        
        if not has_indicator:
            log.debug(f"⚠️  No academic indicators: {npl_text[:60]}...")
            return False
        
        return True
    
    @staticmethod
    def is_valid_author_name(name: str) -> bool:
        """
        Validate that a string is a real person's name, not a product/organization.
        """
        if not name or len(name) < 3 or len(name) > 40:
            return False
        
        name_lower = name.lower().strip()
        
        # CRITICAL: Reject journal abbreviations
        journal_patterns = [
            r'^j\.\s+(cancer|chem|phys|biol|med|appl|mol|cell|exp|clin)$',
            r'^(int|eur|am|can)\.\s+j\.',
            r'^proc\.\s+natl',
            r'^acad\.\s+sci',
        ]
        for pattern in journal_patterns:
            if re.search(pattern, name_lower):
                return False
        
        # Reject product words
        for word in NPLParser.PRODUCT_WORDS:
            if word in name_lower:
                return False
        
        # Reject if contains numbers or special chars (except dots and hyphens)
        if re.search(r'[0-9@#$%&*()]', name):
            return False
        
        # Reject just initials
        if re.match(r'^[A-Z]\.\s*[A-Z]\.$', name):
            return False
        
        # Reject organization words
        org_words = [
            'office', 'action', 'international', 'national',
            'committee', 'forum', 'group', 'administration',
            'designer', 'owner', 'llc', 'inc', 'ltd', 'corp',
            'journal', 'proceedings', 'transactions',
            'enterprises', 'associates', 'solutions', 'systems',
        ]
        for word in org_words:
            if word in name_lower:
                return False
        
        # Must have at least one letter and one space or initial
        if not (re.search(r'[a-zA-Z]', name) and (
            ' ' in name or '.' in name
        )):
            return False

        # Reject if the last word is a common English word (not a known surname)
        # Catches: "C. The", "S. Lab", etc.
        last_word = name.split()[-1].lower()
        if last_word in _ENGLISH_WORDS and last_word not in _SURNAME_EXCEPTIONS:
            return False

        # Reject gerunds/verb forms not in NLTK base-form corpus
        # Catches: "C. Capturing", "E. Introducing", etc.
        if last_word in _VERB_FORMS:
            return False

        # Reject abbreviations and terms that NLTK misses
        # Catches: "H. Ultrasound", "D.I. Seminars", "R. Civ", "N. Engl", etc.
        if last_word in _CURATED_FALSE_POSITIVES:
            return False

        return True
    
    # ============================================================================
    # EXTRACTION
    # ============================================================================
    
    @staticmethod
    def extract_authors(npl_text: str) -> List[str]:
        """
        Extract ONLY real author names from academic papers.
        """
        if not npl_text:
            return []
        
        # FIRST: Skip if not academic
        if NPLParser.should_skip_citation(npl_text):
            return []
        
        # SECOND: Check for academic indicators
        if not NPLParser.is_likely_academic(npl_text):
            return []
        
        # THIRD: Extract authors using academic patterns
        authors = []
        
        # Find where the title/author section ends
        # Academic citations format: "Authors, Title. Journal, Volume, Pages, Year"
        # The title can be quoted OR unquoted
        # Authors always come BEFORE the first sentence-ending period or quote
        
        # Look for title markers (in priority order)
        title_end = len(npl_text)
        
        # 1. Quoted title
        quote_match = re.search(r'["\'«]', npl_text)
        if quote_match:
            title_end = min(title_end, quote_match.start())
        
        # 2. Common journal abbreviations (these mark where authors end)
        journal_markers = [
            r'\bProc\s+Natl\s+Acad\s+Sci',  # Proc Natl Acad Sci
            r'\bInt\.?\s+J\.',  # Int. J.
            r'\bEur\.?\s+J\.',  # Eur. J.
            r'\bJ\.\s+[A-Z]',  # J. Cancer, J. Chem, etc.
            r'\bNature\b',
            r'\bScience\b',
            r'\bIEEE\b',
            r'\bACM\b',
        ]
        for pattern in journal_markers:
            match = re.search(pattern, npl_text)
            if match:
                title_end = min(title_end, match.start())
        
        # 3. Fallback: First 100 characters (conservative)
        title_end = min(title_end, 100)
        
        search_text = npl_text[:title_end]
        
        # Pattern 0: "LastName et al.," (very common in citations)
        pattern_et_al = r'\b([A-Z][a-z]{2,15})\s+et\s+al\.'
        for match in re.finditer(pattern_et_al, search_text):
            last = match.group(1)
            name = last.strip()
            name_lower = name.lower()
            is_english_non_surname = name_lower in _ENGLISH_WORDS and name_lower not in _SURNAME_EXCEPTIONS
            is_asian_given_name = name_lower in NPLParser.KNOWN_GIVEN_NAMES
            if (len(name) > 2
                    and not is_english_non_surname
                    and not is_asian_given_name
                    and name not in authors):
                authors.append(name)
        
        # Pattern 1: "LastName, F. M." (most reliable for academic papers)
        pattern1 = r'\b([A-Z][a-z]{2,15}),\s+([A-Z]\.\s*(?:[A-Z]\.\s*)?)'
        for match in re.finditer(pattern1, search_text):
            last, initials = match.groups()
            name = f"{initials.strip()} {last.strip()}"
            if NPLParser.is_valid_author_name(name) and name not in authors:
                authors.append(name)
        
        # Pattern 2: "F. M. LastName" (common in some journals) 
        pattern2 = r'\b([A-Z]\.\s*(?:[A-Z]\.\s*)?)\s+([A-Z][a-z]{2,15})\b'
        for match in re.finditer(pattern2, search_text):
            initials, last = match.groups()
            name = f"{initials.strip()} {last.strip()}"
            if NPLParser.is_valid_author_name(name) and name not in authors:
                authors.append(name)
        
        # Limit to reasonable number
        authors = authors[:10]

        # Dedup: if a bare last name (e.g. "Peters") and a fuller form (e.g. "E. Peters")
        # both appear in the same citation, keep only the fuller form.
        fuller_lastnames = {
            a.split()[-1].lower()
            for a in authors
            if ' ' in a or '.' in a  # has initials
        }
        authors = [
            a for a in authors
            if (' ' in a or '.' in a)  # always keep names with initials
            or a.lower() not in fuller_lastnames  # keep bare name only if no fuller form exists
        ]

        if authors:
            log.debug(f"✅ Found {len(authors)} authors: {authors[:3]}")
        else:
            log.debug(f"⚠️  No valid authors found in: {npl_text[:60]}...")
        
        return authors
    
    @staticmethod
    def extract_year(npl_text: str) -> Optional[int]:
        """Extract publication year."""
        if not npl_text:
            return None
        
        # Find all 4-digit years
        matches = list(re.finditer(r'\b(19\d{2}|20\d{2})\b', npl_text))
        if matches:
            # Take the last one (usually publication year)
            return int(matches[-1].group(1))
        return None
    
    @staticmethod
    def extract_title(npl_text: str) -> Optional[str]:
        """Extract article title."""
        if not npl_text:
            return None
        
        # Try to find text in quotes
        quote_patterns = [
            r'"([^"]{15,150})"',
            r"'([^']{15,150})'",
            r'«([^»]{15,150})»',
        ]
        
        for pattern in quote_patterns:
            match = re.search(pattern, npl_text)
            if match:
                title = match.group(1).strip()
                # Make sure it's not a product description
                if not any(word in title.lower() for word in NPLParser.PRODUCT_WORDS):
                    return title
        
        # Fallback: take text before first comma or journal name
        parts = npl_text.split(',')
        if len(parts) > 1:
            candidate = parts[0].strip()
            if 20 < len(candidate) < 150:
                return candidate
        
        return None
    
    @staticmethod
    def parse_npl(npl_text: str) -> Dict:
        """
        Parse NPL citation - returns empty authors if not academic paper.
        """
        return {
            'authors': NPLParser.extract_authors(npl_text),
            'title': NPLParser.extract_title(npl_text),
            'year': NPLParser.extract_year(npl_text),
            'raw_text': npl_text,
            'is_academic': NPLParser.is_likely_academic(npl_text) and not NPLParser.should_skip_citation(npl_text),
        }


# ============================================================================
# TESTS
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    test_citations = [
        # ✅ SHOULD EXTRACT AUTHORS (Academic papers)
        "Smith, J. and Doe, A., 'Machine Learning for Patents', Nature, Vol. 123, pp. 45-67, 2023",
        "Johnson, M.A., et al., 'Deep Learning Applications', Science, 2022",
        "Lee, K., Wong, P., 'Quantum Computing Review', IEEE Transactions, 2021",

        # ❌ SHOULD SKIP (Product listings)
        '"Bamboo Wolf Hardside Suitcase", Amazon.ca, visited Dec. 27, 2021',
        'alibaba.com, "Portable Luggage Wheels for Luggage Trolley Bags"',
        'Samsonite Centric, First available Sep. 20, 2017',

        # ❌ SHOULD SKIP (Office actions)
        '2020 Dec. 2020—(CN) First Office Action—App. No. 201880036572.1.',
        'Apr. 19, 2022—(JP) Office Action—App. No. 2021-012889.',

        # ❌ SHOULD SKIP (Standards)
        '"ATM-MPLS Network Interworking Version 2.0" ATM Standard, The ATM Forum Technical Committee, 2003',
        'IEEE Std 802.11-2020, IEEE Standard for Information Technology',

        # ❌ SHOULD SKIP (Design patents)
        'Design No. 307145073, Jun. 16, 2021, China National Intellectual Property Administration',

        # ✅ REAL AUTHOR + false positive in same citation — keep author, reject junk
        'Fonatsch, C. The role of chromosome 3 in cancer, Science, 2021',   # keep "C. Fonatsch", reject "C. The"
        # ✅ Real author + false positive — keep real author, reject abbreviation
        'Mortimer, S. Lab Chip sequencing approach, Nature Methods, 2020',   # keep "S. Mortimer", reject "S. Lab"
        # ✅ Real author + verb form — keep real author, reject gerund
        'Jabara, C. Capturing sequence diversity in metagenomes, Science, 2011',  # keep "C. Jabara", reject "C. Capturing"
        # ❌ Asian given name in "et al." position — must reject
        'Tiesheng et al., Deep sequencing of circulating DNA, Nature, 2019',
        # ❌ Drug name matching surname pattern — must reject
        'Markham, A. Lurbinectedin: A Review of its Use, Lancet Oncol, 2022',  # "A. Lurbinectedin" is a drug, not a name
        # ❌ Journal abbreviation "N. Y. Acad" — must reject
        'Borovikova, L.V. Ann N Y Acad Sci., et al., Nature, 2000',  # "N. Y. Acad" is not a person
        # ❌ Organization word — must reject
        'W.L. Enterprises: Industrial Process Engineering, IEEE Trans, 2021',
        # ❌ Japanese given name in surname position — must reject
        'Akinori et al., CRISPR gene editing in vivo, Nature Methods, 2020',
        # ❌ Italian given name in surname position — must reject
        'Simona et al., CAR-T cell therapy outcomes, Science, 2021',
        # ✅ Peters deduplication: "Peters et al." AND "E. Peters" in same citation — keep only fuller form
        'Peters, E. et al., Adverse events in clinical trials, Lancet, 2019',  # keep "E. Peters", dedupe bare "Peters"
    ]
    
    print("\n" + "="*80)
    print("NPL PARSER VALIDATION TESTS")
    print("="*80)
    
    for i, citation in enumerate(test_citations, 1):
        result = NPLParser.parse_npl(citation)
        
        print(f"\n{i}. {citation[:70]}...")
        print(f"   Is Academic: {result['is_academic']}")
        print(f"   Authors: {result['authors']}")
        print(f"   Title: {result['title']}")
        print(f"   Year: {result['year']}")
        
        # Tests that SHOULD extract at least one valid author
        # 12=Fonatsch(keep C.Fonatsch, reject C.The), 13=Mortimer(keep S.Mortimer, reject S.Lab)
        # 14=Jabara(keep C.Jabara, reject C.Capturing), 17=Borovikova(keep L.V.Borovikova, reject N.Y.Acad)
        # 21=Peters(keep E.Peters, dedupe bare Peters)
        SHOULD_HAVE_AUTHORS = {1, 2, 3, 12, 13, 14, 17, 21}
        if i in SHOULD_HAVE_AUTHORS:
            if not result['authors']:
                print("   ❌ FAIL: Should have extracted authors!")
            else:
                print("   ✅ PASS")
        else:
            if result['authors']:
                print(f"   ❌ FAIL: Should NOT have extracted authors! Got: {result['authors']}")
            else:
                print("   ✅ PASS")