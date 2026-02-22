import os
import sys
import hashlib
import sqlite3
import time
import json
import shutil
import subprocess
import socket
import re
from collections import defaultdict
from firecrawl import FirecrawlApp
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse, quote_plus, unquote, urljoin
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

DEFAULT_CONDITION_TARGETS: Dict[str, Dict[str, Any]] = {
    "me_cfs": {
        "label": "ME/CFS",
        "target_completed_pages": 180,
        "target_cases": 700,
        "seed_queries": ["me/cfs patient stories forum", "myalgic encephalomyelitis lived experience"],
        "seed_domains": ["forums.phoenixrising.me", "www.healingwell.com", "community.patient.info"],
        "url_keywords": ["me-cfs", "chronic-fatigue", "fatigue", "post-viral"],
        "suffering_axes": ["fatigue", "post-exertional malaise", "cognitive load", "care burden"],
    },
    "long_covid": {
        "label": "Long COVID",
        "target_completed_pages": 140,
        "target_cases": 500,
        "seed_queries": ["long covid patient stories forum", "post covid syndrome lived experience"],
        "seed_domains": ["community.patient.info", "healthtalk.org", "forums.phoenixrising.me"],
        "url_keywords": ["long-covid", "post-covid", "covid"],
        "suffering_axes": ["breathlessness", "fatigue", "work loss", "uncertain recovery"],
    },
    "post_viral": {
        "label": "Post-viral Syndromes",
        "target_completed_pages": 120,
        "target_cases": 420,
        "seed_queries": ["post viral syndrome patient stories", "viral onset chronic illness forum"],
        "seed_domains": ["forums.phoenixrising.me", "www.healingwell.com", "community.patient.info"],
        "url_keywords": ["post-viral", "viral", "fatigue", "long-covid"],
        "suffering_axes": ["viral trigger", "relapse cycles", "uncertain prognosis"],
    },
    "chronic_pain": {
        "label": "Chronic Pain",
        "target_completed_pages": 160,
        "target_cases": 600,
        "seed_queries": ["chronic pain forum patient stories", "neuropathic pain lived experience"],
        "seed_domains": ["www.healingwell.com", "healthtalk.org"],
        "url_keywords": ["pain", "chronic-pain", "neuropath", "fibromyalgia"],
        "suffering_axes": ["pain flare", "sleep disruption", "mobility", "opioid tradeoffs"],
    },
    "fibromyalgia": {
        "label": "Fibromyalgia",
        "target_completed_pages": 120,
        "target_cases": 450,
        "seed_queries": ["fibromyalgia patient forum story", "fibromyalgia daily life thread"],
        "seed_domains": ["www.healingwell.com", "community.patient.info"],
        "url_keywords": ["fibromyalgia", "fibro"],
        "suffering_axes": ["pain", "fatigue", "brain fog", "social invisibility"],
    },
    "dysautonomia": {
        "label": "Dysautonomia / POTS",
        "target_completed_pages": 120,
        "target_cases": 450,
        "seed_queries": ["pots dysautonomia patient forum", "orthostatic intolerance lived experience"],
        "seed_domains": ["forums.phoenixrising.me", "www.healingwell.com"],
        "url_keywords": ["dysautonomia", "pots", "orthostatic"],
        "suffering_axes": ["orthostatic symptoms", "heart rate instability", "exercise intolerance"],
    },
    "connective_tissue": {
        "label": "Connective Tissue Disorders",
        "target_completed_pages": 100,
        "target_cases": 350,
        "seed_queries": ["ehlers danlos patient story", "hypermobility syndrome lived experience"],
        "seed_domains": ["www.ehlers-danlos.com", "forums.phoenixrising.me"],
        "url_keywords": ["ehlers", "danlos", "hypermobility", "connective"],
        "suffering_axes": ["joint instability", "chronic pain", "diagnostic delay"],
    },
    "antidepressant_withdrawal": {
        "label": "Antidepressant Withdrawal",
        "target_completed_pages": 180,
        "target_cases": 700,
        "seed_queries": ["antidepressant withdrawal forum", "ssri taper lived experience"],
        "seed_domains": ["www.survivingantidepressants.org"],
        "url_keywords": ["withdrawal", "taper", "ssri", "snri", "topic"],
        "suffering_axes": ["withdrawal symptoms", "taper strategy", "relapse fear"],
    },
    "mental_health": {
        "label": "Mental Health",
        "target_completed_pages": 140,
        "target_cases": 500,
        "seed_queries": ["depression anxiety forum patient stories", "mental health lived experience forum"],
        "seed_domains": ["www.healingwell.com", "community.patient.info"],
        "url_keywords": ["depression", "anxiety", "panic", "mental-health"],
        "suffering_axes": ["anxiety", "depression", "social isolation", "care access"],
    },
    "autoimmune": {
        "label": "Autoimmune Conditions",
        "target_completed_pages": 160,
        "target_cases": 550,
        "seed_queries": ["lupus rheumatoid arthritis patient forum", "autoimmune disease lived experience"],
        "seed_domains": ["www.healingwell.com", "community.patient.info"],
        "url_keywords": ["lupus", "arthritis", "autoimmune", "crohn", "colitis", "ms"],
        "suffering_axes": ["flare cycles", "medication side effects", "diagnostic burden"],
    },
    "gastrointestinal": {
        "label": "Gastrointestinal Conditions",
        "target_completed_pages": 150,
        "target_cases": 500,
        "seed_queries": ["ibs crohn colitis patient forum stories", "gastrointestinal chronic illness experiences"],
        "seed_domains": ["www.healingwell.com", "community.patient.info"],
        "url_keywords": ["ibs", "crohn", "colitis", "gastro", "gerd"],
        "suffering_axes": ["food restriction", "pain", "urgency", "social disruption"],
    },
    "neurological": {
        "label": "Neurological Conditions",
        "target_completed_pages": 130,
        "target_cases": 450,
        "seed_queries": ["multiple sclerosis migraine patient forum", "neurological chronic illness stories"],
        "seed_domains": ["www.healingwell.com", "community.patient.info", "forums.phoenixrising.me"],
        "url_keywords": ["multiple-sclerosis", "migraine", "neurolog", "ms"],
        "suffering_axes": ["cognitive symptoms", "mobility", "uncertain progression"],
    },
    "endocrine": {
        "label": "Endocrine / Metabolic",
        "target_completed_pages": 90,
        "target_cases": 300,
        "seed_queries": ["diabetes forum patient stories", "endocrine chronic illness lived experience"],
        "seed_domains": ["www.healingwell.com", "community.patient.info"],
        "url_keywords": ["diabetes", "glucose", "endocrine"],
        "suffering_axes": ["monitoring burden", "diet constraints", "complication anxiety"],
    },
}

DEFAULT_SOURCES = [
    {
        "name": "Science for ME - Patient Experiences",
        "url": "https://www.s4me.info/forums/patient-experiences-and-stories.18/",
        "queries": ["patient stories", "threads", "experiences"],
        "allow_tokens": ["threads", "experiences", "stories", "patients"],
        "prefer_tokens": ["/threads/"],
        "deny_tokens": ["news-in-brief", "whats-new", "dismiss-notice", "/help/"],
        "hard_deny_tokens": ["news-in-brief", "whats-new", "dismiss-notice"],
        "condition_tags": ["me_cfs"],
        "enabled": False,
        "disabled_reason": "Login wall for anonymous users (verified 2026-02-22).",
    },
    {
        "name": "Science for ME - ME/CFS Coping & Management",
        "url": "https://www.s4me.info/forums/me-cfs-coping-management.8/",
        "queries": ["coping", "thread", "management"],
        "allow_tokens": ["threads", "coping", "management", "me-cfs"],
        "prefer_tokens": ["/threads/"],
        "deny_tokens": ["news-in-brief", "whats-new", "dismiss-notice", "/help/"],
        "hard_deny_tokens": ["news-in-brief", "whats-new", "dismiss-notice"],
        "condition_tags": ["me_cfs"],
        "enabled": False,
        "disabled_reason": "404 forum endpoint (verified 2026-02-22).",
    },
    {
        "name": "Phoenix Rising - Disease Onset & Progression",
        "url": "https://forums.phoenixrising.me/forums/disease-onset-and-progression.254/",
        "queries": ["patient story", "disease onset", "progression"],
        "allow_tokens": ["threads", "disease-onset", "progression", "patient", "story"],
        "prefer_tokens": ["/threads/"],
        "condition_tags": ["me_cfs", "post_viral", "long_covid"],
        "max_urls": 300,
    },
    {
        "name": "Phoenix Rising - Symptoms & Treatments",
        "url": "https://forums.phoenixrising.me/forums/symptoms.31/",
        "queries": ["symptoms", "treatments", "thread"],
        "allow_tokens": ["threads", "symptom", "pain", "dysautonomia", "sleep", "neurological"],
        "prefer_tokens": ["/threads/"],
        "condition_tags": ["me_cfs", "chronic_pain", "dysautonomia", "neurological"],
        "max_urls": 250,
    },
    {
        "name": "Phoenix Rising - Living With ME/CFS",
        "url": "https://forums.phoenixrising.me/forums/living-with-me-cfs.108/",
        "queries": ["living with me/cfs", "daily life", "disability"],
        "allow_tokens": ["threads", "living-with-me-cfs", "disability", "lifestyle", "daily"],
        "prefer_tokens": ["/threads/"],
        "condition_tags": ["me_cfs", "mental_health"],
        "max_urls": 200,
    },
    {
        "name": "Healthtalk.org - Chronic Pain",
        "url": "https://healthtalk.org/chronic-pain/overview",
        "queries": ["experiences", "lived experience", "story"],
        "allow_tokens": ["chronic-pain", "experiences", "living-with", "interview"],
        "prefer_tokens": ["/experiences/", "/interview/"],
        "required_tokens": ["chronic-pain"],
        "condition_tags": ["chronic_pain"],
        "min_score": 12,
        "max_urls": 200,
    },
    {
        "name": "Healthtalk.org - Long COVID",
        "url": "https://healthtalk.org/long-covid/overview",
        "queries": ["experiences", "lived experience", "story"],
        "allow_tokens": ["long-covid", "experiences", "living-with", "interview"],
        "prefer_tokens": ["/experiences/", "/interview/"],
        "required_tokens": ["long-covid"],
        "condition_tags": ["long_covid", "post_viral"],
        "min_score": 12,
        "max_urls": 200,
    },
    {
        "name": "Dysautonomia International - Patient Stories",
        "url": "http://www.dysautonomiainternational.org/page.php?ID=14",
        "queries": ["patient stories", "story", "personal experience"],
        "allow_tokens": ["patient", "story", "dysautonomia", "page.php"],
        "prefer_tokens": ["page.php?id=14", "story"],
        "min_score": 12,
        "max_urls": 120,
        "condition_tags": ["dysautonomia"],
        "enabled": False,
        "disabled_reason": "Current endpoint maps mostly organizational pages, not patient narratives (verified 2026-02-22).",
    },
    {
        "name": "Surviving Antidepressants - Introductions and Updates",
        "url": "https://www.survivingantidepressants.org/forum/3-introductions-and-updates/",
        "queries": ["topic", "introduction", "experience"],
        "allow_tokens": ["topic", "introductions", "updates"],
        "prefer_tokens": ["/topic/"],
        "condition_tags": ["antidepressant_withdrawal", "mental_health"],
    },
    {
        "name": "Ehlers-Danlos Society - Our Stories",
        "url": "https://www.ehlers-danlos.com/our-stories/",
        "queries": ["our stories", "patient stories", "journey"],
        "allow_tokens": ["our-stories", "story", "patient"],
        "prefer_tokens": ["/story/"],
        "condition_tags": ["connective_tissue", "dysautonomia", "chronic_pain"],
    },
    {
        "name": "HealingWell - Chronic Fatigue Syndrome Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=15",
        "queries": ["chronic fatigue", "daily life", "symptoms"],
        "allow_tokens": ["default.aspx", "f=15", "m=", "fatigue", "chronic"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=15"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["me_cfs", "post_viral"],
        "min_score": 8,
        "max_urls": 220,
    },
    {
        "name": "HealingWell - Chronic Pain Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=16",
        "queries": ["chronic pain", "symptoms", "coping"],
        "allow_tokens": ["default.aspx", "f=16", "m=", "pain"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=16"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["chronic_pain"],
        "min_score": 8,
        "max_urls": 220,
    },
    {
        "name": "HealingWell - Fibromyalgia Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=24",
        "queries": ["fibromyalgia", "pain", "fatigue"],
        "allow_tokens": ["default.aspx", "f=24", "m=", "fibromyalgia", "fibro"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=24"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["fibromyalgia", "chronic_pain"],
        "min_score": 8,
        "max_urls": 220,
    },
    {
        "name": "HealingWell - Anxiety & Panic Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=9",
        "queries": ["anxiety", "panic", "daily functioning"],
        "allow_tokens": ["default.aspx", "f=9", "m=", "anxiety", "panic"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=9"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["mental_health"],
        "min_score": 8,
        "max_urls": 200,
    },
    {
        "name": "HealingWell - Depression Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=19",
        "queries": ["depression", "lived experience", "daily struggle"],
        "allow_tokens": ["default.aspx", "f=19", "m=", "depression"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=19"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["mental_health"],
        "min_score": 8,
        "max_urls": 200,
    },
    {
        "name": "HealingWell - Lupus Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=29",
        "queries": ["lupus flare", "symptoms", "treatment"],
        "allow_tokens": ["default.aspx", "f=29", "m=", "lupus"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=29"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["autoimmune"],
        "min_score": 8,
        "max_urls": 200,
    },
    {
        "name": "HealingWell - Rheumatoid Arthritis Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=10",
        "queries": ["rheumatoid arthritis", "flare", "pain"],
        "allow_tokens": ["default.aspx", "f=10", "m=", "arthritis", "ra"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=10"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["autoimmune", "chronic_pain"],
        "min_score": 8,
        "max_urls": 200,
    },
    {
        "name": "HealingWell - IBS Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=26",
        "queries": ["ibs symptoms", "diet", "daily life"],
        "allow_tokens": ["default.aspx", "f=26", "m=", "ibs", "bowel"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=26"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["gastrointestinal"],
        "min_score": 8,
        "max_urls": 200,
    },
    {
        "name": "HealingWell - Crohn's Disease Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=17",
        "queries": ["crohns flare", "symptoms", "treatment"],
        "allow_tokens": ["default.aspx", "f=17", "m=", "crohn"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=17"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["gastrointestinal", "autoimmune"],
        "min_score": 8,
        "max_urls": 200,
    },
    {
        "name": "HealingWell - Ulcerative Colitis Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=38",
        "queries": ["ulcerative colitis", "flare", "symptoms"],
        "allow_tokens": ["default.aspx", "f=38", "m=", "colitis"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=38"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["gastrointestinal", "autoimmune"],
        "min_score": 8,
        "max_urls": 200,
    },
    {
        "name": "HealingWell - Multiple Sclerosis Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=32",
        "queries": ["multiple sclerosis", "symptoms", "progression"],
        "allow_tokens": ["default.aspx", "f=32", "m=", "multiple-sclerosis", "ms"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=32"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["neurological", "autoimmune"],
        "min_score": 8,
        "max_urls": 200,
    },
    {
        "name": "HealingWell - Diabetes Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=20",
        "queries": ["diabetes management", "glucose", "daily life"],
        "allow_tokens": ["default.aspx", "f=20", "m=", "diabetes"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=20"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["endocrine"],
        "min_score": 8,
        "max_urls": 200,
    },
    {
        "name": "HealingWell - GERD & Acid Reflux Forum",
        "url": "https://www.healingwell.com/community/default.aspx?f=45",
        "queries": ["gerd reflux", "symptoms", "lived experience"],
        "allow_tokens": ["default.aspx", "f=45", "m=", "gerd", "reflux"],
        "prefer_tokens": ["m="],
        "deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "hard_deny_tokens": ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"],
        "required_tokens": ["f=45"],
        "require_story_like_urls": True,
        "allow_seed_fallback": False,
        "condition_tags": ["gastrointestinal"],
        "min_score": 8,
        "max_urls": 200,
    },
]

DEFAULT_DISEASE_LIST_SEED_URLS = [
    "https://www.healingwell.com/conditions/",
    "https://community.patient.info/tags",
]

CONDITION_TARGETS: Dict[str, Dict[str, Any]] = json.loads(json.dumps(DEFAULT_CONDITION_TARGETS))
SOURCES: List[Dict[str, Any]] = json.loads(json.dumps(DEFAULT_SOURCES))
DISEASE_LIST_SEED_URLS: List[str] = list(DEFAULT_DISEASE_LIST_SEED_URLS)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "public_data" / "scraped"
REJECTED_DIR = BASE_DIR / "public_data" / "scraped_rejected"
DB_PATH = BASE_DIR / "public_data" / "crawl_state.db"
REGISTRY_PATH = Path(os.environ.get("CRAWL_SOURCE_REGISTRY_PATH", str(BASE_DIR / "config" / "source_registry.json")))
DISCOVERED_SEEDS_PATH = BASE_DIR / "public_data" / "discovered_seed_candidates.json"
MAX_RETRIES = int(os.environ.get("CRAWL_MAX_RETRIES", "3"))
MAP_DELAY_SECONDS = float(os.environ.get("CRAWL_MAP_DELAY_SECONDS", "2"))
CHUNK_DELAY_SECONDS = float(os.environ.get("CRAWL_CHUNK_DELAY_SECONDS", "0.5"))
MAP_LIMIT_PER_CALL = int(os.environ.get("CRAWL_MAP_LIMIT_PER_CALL", "2000"))
MAX_URLS_PER_SOURCE = int(os.environ.get("CRAWL_MAX_URLS_PER_SOURCE", "1200"))
MIN_RELEVANCE_SCORE = int(os.environ.get("CRAWL_MIN_RELEVANCE_SCORE", "6"))
MIN_FALLBACK_HIGH_SIGNAL_LINKS = int(os.environ.get("CRAWL_MIN_FALLBACK_HIGH_SIGNAL_LINKS", "3"))
MIN_MARKDOWN_CHARS = int(os.environ.get("CRAWL_MIN_MARKDOWN_CHARS", "250"))
MIN_MARKDOWN_CHARS_FALLBACK = int(os.environ.get("CRAWL_MIN_MARKDOWN_CHARS_FALLBACK", "120"))
PROCESSING_STALE_HOURS = int(os.environ.get("CRAWL_PROCESSING_STALE_HOURS", "2"))
MAX_SOURCES = int(os.environ.get("CRAWL_MAX_SOURCES", "0"))
EARLY_STOP_MAP_ZERO_STREAK = int(os.environ.get("CRAWL_EARLY_STOP_MAP_ZERO_STREAK", "2"))
QUARANTINE_SUSPECT_OUTPUTS = os.environ.get("CRAWL_QUARANTINE_SUSPECT_OUTPUTS", "1").lower() not in {"0", "false", "no"}
SEED_PROBE_ENABLED = os.environ.get("CRAWL_SEED_PROBE_ENABLED", "1").lower() not in {"0", "false", "no"}
SEED_PROBE_MIN_HIGH_SIGNAL_LINKS = int(os.environ.get("CRAWL_SEED_PROBE_MIN_HIGH_SIGNAL_LINKS", "4"))
SEED_PROBE_MIN_HIGH_SIGNAL_RATIO = float(os.environ.get("CRAWL_SEED_PROBE_MIN_HIGH_SIGNAL_RATIO", "0.06"))
SEED_PROBE_MAP_ENABLED = os.environ.get("CRAWL_SEED_PROBE_MAP_ENABLED", "0").lower() not in {"0", "false", "no"}
DISCOVERY_ENABLED = os.environ.get("CRAWL_DISCOVERY_ENABLED", "0").lower() not in {"0", "false", "no"}
DISCOVERY_MAX_CANDIDATES_PER_CONDITION = int(os.environ.get("CRAWL_DISCOVERY_MAX_CANDIDATES_PER_CONDITION", "10"))
DISCOVERY_SEARCH_RESULTS_PER_QUERY = int(os.environ.get("CRAWL_DISCOVERY_SEARCH_RESULTS_PER_QUERY", "10"))
DISCOVERY_SITEMAP_URL_LIMIT = int(os.environ.get("CRAWL_DISCOVERY_SITEMAP_URL_LIMIT", "150"))
DISCOVERY_HTTP_TIMEOUT_SECONDS = float(os.environ.get("CRAWL_DISCOVERY_HTTP_TIMEOUT_SECONDS", "8"))
DISCOVERY_MAX_SEED_HOSTS_PER_CONDITION = int(os.environ.get("CRAWL_DISCOVERY_MAX_SEED_HOSTS_PER_CONDITION", "6"))
DISCOVERY_SITEMAP_MAX_XMLS_PER_DOMAIN = int(os.environ.get("CRAWL_DISCOVERY_SITEMAP_MAX_XMLS_PER_DOMAIN", "8"))
DISCOVERY_MIN_CANDIDATE_SCORE = int(os.environ.get("CRAWL_DISCOVERY_MIN_CANDIDATE_SCORE", "7"))
PARSER_CONTINUOUS_ENABLED = os.environ.get("CRAWL_RUN_PARSER_CONTINUOUS", "1").lower() not in {"0", "false", "no"}
PARSER_MIN_INTERVAL_SECONDS = int(os.environ.get("CRAWL_PARSER_MIN_INTERVAL_SECONDS", "120"))
_LAST_PARSER_RUN_TS = 0.0
_PARSER_WARNED_MISSING_KEY = False

HIGH_SIGNAL_URL_TOKENS = (
    "patient",
    "patients",
    "story",
    "stories",
    "journey",
    "experience",
    "experiences",
    "diagnosis",
    "symptom",
    "symptoms",
    "treatment",
    "treatments",
    "long-covid",
    "me-cfs",
    "chronic-pain",
    "fibromyalgia",
    "dysautonomia",
)

LOW_SIGNAL_URL_TOKENS = (
    "login",
    "logout",
    "register",
    "signup",
    "sign-up",
    "privacy",
    "terms",
    "cookies",
    "contact",
    "about-us",
    "members",
    "member",
    "profile",
    "search",
    "tag/",
    "/tags",
    "/feed",
    "/rss",
    "wp-json",
    "wp-admin",
    "calendar",
    "whats-new",
    "dismiss-notice",
    "lost-password",
    "announcement",
    "news-in-brief",
    "/account/",
    "/help/",
    "/members/",
    "profile.aspx",
    "login.aspx",
    "register.aspx",
    "post.aspx?f=",
)

DISCOVERY_JUNK_HOST_MARKERS = (
    "google.com",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "youtube.com",
    "tiktok.com",
    "x.com",
    "twitter.com",
    "bsky.app",
    "donatestock.com",
)

DISCOVERY_JUNK_URL_TOKENS = (
    "recaptcha",
    "/admin/migrate",
    "oauth",
    "newsletter",
    "/rss",
    "index.rss",
)

TRACKING_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
    "ref",
    "source",
}

BINARY_FILE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".pdf",
    ".zip", ".rar", ".7z", ".mp4", ".mp3", ".avi", ".mov",
    ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
}

INTERSTITIAL_HARD_MARKERS = (
    "access denied",
    "verify you are human",
    "checking if the site connection is secure",
    "just a moment",
)

INTERSTITIAL_SOFT_MARKERS = (
    "enable javascript",
    "captcha",
    "cloudflare",
    "bot protection",
)

ERROR_PAGE_MARKERS = (
    "oops! we ran into some problems",
    "the requested forum could not be found",
    "page not found",
    "404 not found",
)

AUTH_WALL_HARD_MARKERS = (
    "you must be logged-in to do that",
    "log in | science for me",
    "# log in",
)

AUTH_WALL_SOFT_MARKERS = (
    "log in",
    "register",
    "forgot your password",
    "stay logged in",
    "you must be logged-in",
)

DIRECTORY_CONTENT_MARKERS = (
    "topics in this forum",
    "sort by",
    "new posts",
    "search forums",
    "install the app",
    "first page",
    "last page",
    "page 1 of",
)

class FirecrawlConnectivityError(RuntimeError):
    pass

class FirecrawlAuthenticationError(RuntimeError):
    pass

def _source_host(url: str) -> str:
    return urlparse(url).netloc.lower()

def _looks_like_auth_error(message: str) -> bool:
    normalized = message.lower()
    auth_markers = (
        "unauthorized",
        "invalid token",
        "401",
        "api key",
        "forbidden",
    )
    return any(marker in normalized for marker in auth_markers)

def _json_clone(payload: Any) -> Any:
    return json.loads(json.dumps(payload))

def _sanitize_source_entry(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    name = raw.get("name")
    url = raw.get("url")
    if not isinstance(name, str) or not name.strip():
        return None
    if not isinstance(url, str) or not _canonicalize_url(url):
        return None

    source = dict(raw)
    source["name"] = name.strip()
    source["url"] = _canonicalize_url(url)
    queries = source.get("queries", [])
    source["queries"] = [q.strip() for q in queries if isinstance(q, str) and q.strip()]

    for key in ("allow_tokens", "prefer_tokens", "deny_tokens", "hard_deny_tokens", "required_tokens", "condition_tags"):
        values = source.get(key, [])
        if not isinstance(values, list):
            source[key] = []
            continue
        source[key] = [value.strip() for value in values if isinstance(value, str) and value.strip()]

    return source

def _normalize_condition_targets(raw: Any) -> Dict[str, Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = _json_clone(DEFAULT_CONDITION_TARGETS)
    if not isinstance(raw, dict):
        return merged

    for condition_id, payload in raw.items():
        if not isinstance(condition_id, str) or not condition_id.strip():
            continue
        key = condition_id.strip()
        base = merged.get(key, {})
        candidate = dict(base)
        if isinstance(payload, dict):
            label = payload.get("label")
            if isinstance(label, str) and label.strip():
                candidate["label"] = label.strip()
            for metric in ("target_completed_pages", "target_cases"):
                value = payload.get(metric)
                if isinstance(value, int) and value >= 0:
                    candidate[metric] = value
            for list_key in ("seed_queries", "seed_domains", "url_keywords", "suffering_axes"):
                values = payload.get(list_key)
                if isinstance(values, list):
                    candidate[list_key] = [
                        value.strip()
                        for value in values
                        if isinstance(value, str) and value.strip()
                    ]
        merged[key] = candidate
    return merged

def _normalize_seed_url_list(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return list(DEFAULT_DISEASE_LIST_SEED_URLS)
    cleaned: List[str] = []
    for entry in raw:
        if not isinstance(entry, str):
            continue
        canonical = _canonicalize_url(entry)
        if canonical:
            cleaned.append(canonical)
    deduped = list(dict.fromkeys(cleaned))
    return deduped or list(DEFAULT_DISEASE_LIST_SEED_URLS)

def _default_registry_payload() -> Dict[str, Any]:
    return {
        "version": 1,
        "notes": (
            "Source registry for crawl seeds. "
            "Sources carry condition tags. Condition targets define coverage quotas."
        ),
        "disease_list_seed_urls": list(DEFAULT_DISEASE_LIST_SEED_URLS),
        "condition_targets": _json_clone(DEFAULT_CONDITION_TARGETS),
        "sources": _json_clone(DEFAULT_SOURCES),
    }

def load_source_registry(registry_path: Optional[Path] = None) -> Path:
    global SOURCES, CONDITION_TARGETS, DISEASE_LIST_SEED_URLS
    path = registry_path or REGISTRY_PATH
    payload: Optional[Dict[str, Any]] = None

    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                payload = loaded
        except Exception as e:
            print(f"Warning: failed to parse source registry at {path}: {e}")

    if payload is None:
        payload = _default_registry_payload()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"Wrote default source registry to {path}")

    raw_sources = payload.get("sources", [])
    sanitized_sources: List[Dict[str, Any]] = []
    if isinstance(raw_sources, list):
        for entry in raw_sources:
            if isinstance(entry, dict):
                sanitized = _sanitize_source_entry(entry)
                if sanitized is not None:
                    sanitized_sources.append(sanitized)

    if not sanitized_sources:
        sanitized_sources = _json_clone(DEFAULT_SOURCES)
        print("Warning: registry had no valid sources; using built-in defaults.")

    disease_seed_urls = _normalize_seed_url_list(payload.get("disease_list_seed_urls"))
    condition_targets = _normalize_condition_targets(payload.get("condition_targets"))
    SOURCES = sanitized_sources
    CONDITION_TARGETS = condition_targets
    DISEASE_LIST_SEED_URLS = disease_seed_urls
    print(
        f"Loaded source registry: {len(SOURCES)} sources, "
        f"{len(CONDITION_TARGETS)} condition targets, "
        f"{len(DISEASE_LIST_SEED_URLS)} disease-list seeds."
    )
    return path

def _source_condition_tags(source: Dict[str, Any]) -> List[str]:
    tags = source.get("condition_tags", [])
    if not isinstance(tags, list):
        return []
    cleaned = [tag.strip().lower() for tag in tags if isinstance(tag, str) and tag.strip()]
    return list(dict.fromkeys(cleaned))

def _requested_discovery_conditions() -> List[str]:
    raw = os.environ.get("CRAWL_DISCOVERY_CONDITIONS", "").strip()
    if not raw:
        return []
    requested = [item.strip().lower() for item in raw.split(",") if item.strip()]
    return [condition_id for condition_id in requested if condition_id in CONDITION_TARGETS]

def _is_story_like_url(url: str, source: Dict[str, Any]) -> bool:
    url_lower = url.lower()
    default_story_tokens = (
        "/threads/",
        "/thread/",
        "/topic/",
        "/topics/",
        "/story/",
        "/stories/",
        "/journey/",
        "/interview/",
        "m=",
    )
    if any(token in url_lower for token in default_story_tokens):
        return True

    source_tokens = source.get("prefer_tokens", [])
    return any(isinstance(token, str) and token.lower() in url_lower for token in source_tokens)

def _is_forum_listing_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(token in path for token in ("/forums/", "/forum/", "/categories/"))

def _allow_seed_fallback(source: Dict[str, Any]) -> bool:
    configured = source.get("allow_seed_fallback")
    if isinstance(configured, bool):
        return configured
    return not _is_forum_listing_url(source.get("url", ""))

def _source_is_enabled(source: Dict[str, Any]) -> bool:
    configured = source.get("enabled")
    if configured is None:
        return True
    return bool(configured)

def _source_min_score(source: Dict[str, Any]) -> int:
    configured = source.get("min_score")
    if isinstance(configured, int) and configured > 0:
        return configured
    return MIN_RELEVANCE_SCORE

def _source_requires_story_like_urls(source: Dict[str, Any]) -> bool:
    configured = source.get("require_story_like_urls")
    if isinstance(configured, bool):
        return configured
    return _is_forum_listing_url(source.get("url", ""))

def _source_seed_probe_thresholds(source: Dict[str, Any]) -> Tuple[int, float]:
    min_links = source.get("seed_probe_min_high_signal_links")
    min_ratio = source.get("seed_probe_min_high_signal_ratio")

    resolved_min_links = (
        int(min_links)
        if isinstance(min_links, int) and min_links > 0
        else SEED_PROBE_MIN_HIGH_SIGNAL_LINKS
    )
    resolved_min_ratio = (
        float(min_ratio)
        if isinstance(min_ratio, (int, float)) and float(min_ratio) >= 0
        else SEED_PROBE_MIN_HIGH_SIGNAL_RATIO
    )
    return resolved_min_links, resolved_min_ratio

def _source_force_include(source: Dict[str, Any]) -> bool:
    configured = source.get("seed_probe_force_include")
    return bool(configured) if isinstance(configured, bool) else False

def _decide_seed_probe_recommendation(
    source: Dict[str, Any],
    high_signal_count: int,
    high_signal_ratio: float,
    seed_score: int,
) -> Tuple[bool, str]:
    if _source_force_include(source):
        return True, "seed_probe_force_include=true"

    min_links, min_ratio = _source_seed_probe_thresholds(source)
    if _is_forum_listing_url(source.get("url", "")):
        if high_signal_count < min_links:
            return False, f"high_signal_count={high_signal_count} < required={min_links}"
        if high_signal_ratio < min_ratio:
            return False, f"high_signal_ratio={high_signal_ratio:.3f} < required={min_ratio:.3f}"
        return True, "forum seed passed high-signal count and ratio checks"

    min_score = _source_min_score(source)
    if high_signal_count >= min_links:
        return True, f"high_signal_count={high_signal_count} >= required={min_links}"
    if seed_score >= min_score:
        return True, f"seed_score={seed_score} >= min_score={min_score}"
    return False, (
        f"high_signal_count={high_signal_count} < required={min_links} "
        f"and seed_score={seed_score} < min_score={min_score}"
    )

def _extract_links(map_result: Any) -> List[str]:
    if isinstance(map_result, dict):
        links = map_result.get("links", [])
    else:
        links = getattr(map_result, "links", [])
    if not isinstance(links, list):
        return []
    return [url for url in links if isinstance(url, str)]

def _extract_markdown(scrape_result: Any) -> str:
    if isinstance(scrape_result, dict):
        markdown = scrape_result.get("markdown", "")
    else:
        markdown = getattr(scrape_result, "markdown", "")
    return markdown if isinstance(markdown, str) else ""

def _extract_source_url_from_markdown(markdown_content: str) -> str:
    match = re.search(r"<!-- SOURCE_URL:\s*(.*?)\s*-->", markdown_content)
    if not match:
        return ""
    extracted = match.group(1).strip()
    return _canonicalize_url(extracted) or extracted

def _canonicalize_url(url: str) -> str:
    if not isinstance(url, str):
        return ""
    candidate = url.strip()
    if not candidate:
        return ""

    parsed = urlparse(candidate)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return ""

    normalized_path = parsed.path
    # Collapse common forum pagination/post anchors back to canonical thread/topic URL.
    normalized_path = re.sub(r"/post-\d+/?$", "", normalized_path)
    normalized_path = re.sub(r"/latest/?$", "", normalized_path)
    normalized_path = re.sub(r"/page-\d+/?$", "", normalized_path)
    normalized_path = re.sub(r"/page/\d+/?$", "", normalized_path)

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    kept_query = [(k, v) for k, v in query_pairs if k.lower() not in TRACKING_QUERY_KEYS]
    if "/threads/" in normalized_path or "/topic/" in normalized_path or "/topics/" in normalized_path:
        kept_query = [(k, v) for k, v in kept_query if k.lower() not in {"page", "p"}]
    query_keys = {k.lower() for k, _v in kept_query}
    if "m" in query_keys:
        # HealingWell thread URLs are default.aspx?f=<forum>&m=<thread>&p=<page>.
        kept_query = [(k, v) for k, v in kept_query if k.lower() not in {"page", "p"}]
    if normalized_path not in {"", "/"}:
        normalized_path = normalized_path.rstrip("/")
    normalized = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        path=normalized_path,
        query=urlencode(kept_query, doseq=True),
        fragment="",
    )
    canonical = urlunparse(normalized)
    return canonical

def _has_binary_extension(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in BINARY_FILE_EXTENSIONS)

def _score_url(url: str, source: Dict[str, Any]) -> int:
    parsed = urlparse(url)
    path = parsed.path.lower()
    query_lower = parsed.query.lower()
    url_lower = url.lower()
    score = 0

    hard_deny_tokens = source.get("hard_deny_tokens", [])
    if any(isinstance(token, str) and token.lower() in url_lower for token in hard_deny_tokens):
        return -100

    source_host = _source_host(source["url"])
    if parsed.netloc.lower() == source_host:
        score += 4

    if _has_binary_extension(url):
        return -100

    is_story_like = _is_story_like_url(url, source)
    if is_story_like:
        score += 20

    if "/threads/" in path or "/topic/" in path:
        score += 12
    if "m=" in query_lower:
        score += 14
    if "f=" in query_lower and "m=" not in query_lower:
        score -= 8
    if "/forums/" in path or "/forum/" in path:
        score += 4
        if not is_story_like:
            score -= 12

    if "/page/" in path or "page-" in path or "page=" in query_lower or "&p=" in query_lower:
        if not is_story_like:
            score -= 10

    high_signal_hits = sum(1 for token in HIGH_SIGNAL_URL_TOKENS if token in url_lower)
    score += min(24, high_signal_hits * 3)

    low_signal_hits = sum(1 for token in LOW_SIGNAL_URL_TOKENS if token in url_lower)
    score -= min(40, low_signal_hits * 8)

    if url == _canonicalize_url(source["url"]) and not is_story_like:
        score -= 2

    allow_tokens = source.get("allow_tokens", [])
    deny_tokens = source.get("deny_tokens", [])
    score += sum(4 for token in allow_tokens if token.lower() in url_lower)
    score -= sum(10 for token in deny_tokens if token.lower() in url_lower)

    if any(marker in query_lower for marker in ("sort", "advancedsearchform", "view", "filter")):
        score -= 8

    if parsed.query and "?" in url and not any(key in query_lower for key in ("id=", "page=", "p=", "m=")):
        score -= 4

    return score

def _rank_candidate_urls(raw_urls: Iterable[str], source: Dict[str, Any]) -> List[Tuple[str, int]]:
    seed_host = _source_host(source["url"])
    ranked: Dict[str, int] = {}
    required_tokens = [
        token.strip().lower()
        for token in source.get("required_tokens", [])
        if isinstance(token, str) and token.strip()
    ]
    min_score = _source_min_score(source)
    require_story_like = _source_requires_story_like_urls(source)

    for raw_url in raw_urls:
        normalized = _canonicalize_url(raw_url)
        if not normalized:
            continue

        parsed_host = _source_host(normalized)
        if parsed_host != seed_host:
            continue
        if required_tokens and not any(token in normalized.lower() for token in required_tokens):
            continue
        if require_story_like and not _is_story_like_url(normalized, source):
            continue

        score = _score_url(normalized, source)
        current = ranked.get(normalized)
        if current is None or score > current:
            ranked[normalized] = score

    filtered = [(url, score) for url, score in ranked.items() if score >= min_score]
    filtered.sort(key=lambda item: (-item[1], item[0]))
    return filtered

def _source_queries(source: Dict[str, Any]) -> List[Optional[str]]:
    queries = [None]
    for query in source.get("queries", []):
        if isinstance(query, str) and query.strip():
            queries.append(query.strip())
    return queries

def _source_map_delay_seconds(source: Dict[str, Any]) -> float:
    configured = source.get("map_delay_seconds")
    if isinstance(configured, (int, float)) and float(configured) >= 0:
        return float(configured)

    host = _source_host(source.get("url", ""))
    if any(
        marker in host
        for marker in (
            "healingwell.com",
            "forums.phoenixrising.me",
            "survivingantidepressants.org",
        )
    ):
        return min(MAP_DELAY_SECONDS, 0.75)
    if _is_forum_listing_url(source.get("url", "")):
        return min(MAP_DELAY_SECONDS, 1.0)
    return MAP_DELAY_SECONDS

def _min_markdown_chars_for_url(url: str) -> int:
    url_lower = url.lower()
    if any(token in url_lower for token in ("thread", "topic", "story", "journey", "experience", "m=")):
        return MIN_MARKDOWN_CHARS
    return MIN_MARKDOWN_CHARS_FALLBACK

def _looks_like_interstitial(markdown_content: str) -> bool:
    content = markdown_content.lower()
    if any(marker in content for marker in INTERSTITIAL_HARD_MARKERS):
        return True

    soft_hits = sum(1 for marker in INTERSTITIAL_SOFT_MARKERS if marker in content)
    short_page = len(content) < 8000
    marker_dense_header = sum(1 for marker in INTERSTITIAL_SOFT_MARKERS if marker in content[:2000]) >= 2
    return soft_hits >= 2 and (short_page or marker_dense_header)

def _looks_like_error_page(markdown_content: str) -> bool:
    content = markdown_content.lower()
    return any(marker in content for marker in ERROR_PAGE_MARKERS)

def _looks_like_auth_wall(markdown_content: str) -> bool:
    content = markdown_content.lower()
    if any(marker in content for marker in AUTH_WALL_HARD_MARKERS):
        return True

    soft_hits = sum(1 for marker in AUTH_WALL_SOFT_MARKERS if marker in content)
    short_page = len(content) < 18000
    heading_login = bool(re.search(r"(^|\n)#+\s*log in\b", content))
    return heading_login or (soft_hits >= 3 and short_page and "xenforo" in content)

def _looks_like_directory_page(markdown_content: str, url: str) -> bool:
    content_lower = markdown_content.lower()
    lines = [line.strip() for line in markdown_content.splitlines() if line.strip()]
    if len(lines) < 35:
        return False

    linkish_lines = 0
    narrative_lines = 0
    for line in lines:
        if line.startswith("- [") or line.startswith("[") or line.startswith("|"):
            linkish_lines += 1
            continue
        if len(line) > 100 and not re.search(r"https?://", line):
            narrative_lines += 1

    ratio = linkish_lines / len(lines)
    url_lower = url.lower()
    parsed = urlparse(url)
    query_lower = parsed.query.lower()
    healingwell_listing = (
        "default.aspx" in parsed.path.lower()
        and "f=" in query_lower
        and "m=" not in query_lower
    )
    listing_context = (
        any(token in url_lower for token in ("/forums/", "/forum/", "/categories/", "/page/", "page-"))
        or healingwell_listing
        or any(token in content_lower for token in DIRECTORY_CONTENT_MARKERS)
    )
    return listing_context and ratio > 0.58 and narrative_lines < 6

def _is_suspect_markdown_content(markdown_content: str, url_hint: str = "") -> bool:
    return (
        _looks_like_interstitial(markdown_content)
        or _looks_like_auth_wall(markdown_content)
        or _looks_like_error_page(markdown_content)
        or _looks_like_directory_page(markdown_content, url_hint)
    )

def _output_path_for_url(url: str) -> Path:
    url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()
    return DATA_DIR / f"{url_hash}.md"

def _move_to_rejected(file_path: Path) -> Optional[Path]:
    if not file_path.exists():
        return None

    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    target_path = REJECTED_DIR / file_path.name
    if target_path.exists():
        stem = file_path.stem
        suffix = file_path.suffix
        index = 1
        while target_path.exists():
            target_path = REJECTED_DIR / f"{stem}.q{index}{suffix}"
            index += 1
    file_path.replace(target_path)
    return target_path

def apply_disabled_source_filters(conn: sqlite3.Connection) -> int:
    enabled_names = {source.get("name") for source in SOURCES if _source_is_enabled(source)}
    if not enabled_names:
        return 0

    cursor = conn.cursor()
    cursor.execute("SELECT url, source_name, status, retry_count FROM urls")
    rows = cursor.fetchall()

    updated = 0
    for url, source_name, status, retry_count in rows:
        if source_name in enabled_names:
            continue

        if status != "FAILED" or int(retry_count or 0) < MAX_RETRIES:
            conn.execute(
                "UPDATE urls SET status = 'FAILED', retry_count = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?",
                (MAX_RETRIES, url),
            )
            updated += 1

        _move_to_rejected(_output_path_for_url(url))

    return updated

def quarantine_suspect_outputs(conn: sqlite3.Connection) -> int:
    if not DATA_DIR.exists():
        return 0

    moved = 0

    for file_path in sorted(DATA_DIR.glob("*.md")):
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        source_url = _extract_source_url_from_markdown(content)
        url_hint = source_url or ""
        if not _is_suspect_markdown_content(content, url_hint):
            continue

        moved_path = _move_to_rejected(file_path)
        if moved_path is None:
            continue
        moved += 1

        if source_url:
            conn.execute(
                "UPDATE urls SET status = 'FAILED', retry_count = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?",
                (MAX_RETRIES, source_url),
            )

    return moved

def _status_priority(status: str) -> int:
    order = {
        "PENDING": 4,
        "PROCESSING": 3,
        "FAILED": 2,
        "COMPLETED": 1,
    }
    return order.get(status, 0)

def normalize_existing_urls(conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT url, source_name, status, discovered_at, processed_at, retry_count, priority FROM urls"
    )
    rows = cursor.fetchall()
    if not rows:
        return 0

    aggregated: Dict[str, Dict[str, Any]] = {}
    for url, source_name, status, discovered_at, processed_at, retry_count, priority in rows:
        canonical = _canonicalize_url(url) or url
        existing = aggregated.get(canonical)
        candidate = {
            "url": canonical,
            "source_name": source_name,
            "status": status,
            "discovered_at": discovered_at,
            "processed_at": processed_at,
            "retry_count": int(retry_count or 0),
            "priority": int(priority or 0),
        }

        if existing is None:
            aggregated[canonical] = candidate
            continue

        if _status_priority(candidate["status"]) > _status_priority(existing["status"]):
            existing["status"] = candidate["status"]
        if candidate["source_name"] and not existing["source_name"]:
            existing["source_name"] = candidate["source_name"]
        existing["retry_count"] = max(existing["retry_count"], candidate["retry_count"])
        existing["priority"] = max(existing["priority"], candidate["priority"])

        if candidate["discovered_at"] and (
            not existing["discovered_at"] or candidate["discovered_at"] < existing["discovered_at"]
        ):
            existing["discovered_at"] = candidate["discovered_at"]
        if candidate["processed_at"] and (
            not existing["processed_at"] or candidate["processed_at"] > existing["processed_at"]
        ):
            existing["processed_at"] = candidate["processed_at"]

    had_noncanonical = any((_canonicalize_url(row[0]) or row[0]) != row[0] for row in rows)
    had_duplicates = len(aggregated) < len(rows)
    if not had_noncanonical and not had_duplicates:
        return 0

    conn.execute("DELETE FROM urls")
    for record in aggregated.values():
        conn.execute(
            '''
            INSERT INTO urls (url, source_name, status, discovered_at, processed_at, retry_count, priority)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                record["url"],
                record["source_name"],
                record["status"],
                record["discovered_at"],
                record["processed_at"],
                record["retry_count"],
                record["priority"],
            ),
        )
    return len(rows) - len(aggregated)

def apply_hard_deny_filters(conn: sqlite3.Connection) -> int:
    source_index = {source.get("name"): source for source in SOURCES}
    cursor = conn.cursor()
    cursor.execute("SELECT url, source_name, status, retry_count FROM urls")
    rows = cursor.fetchall()

    updated = 0
    for url, source_name, status, retry_count in rows:
        source = source_index.get(source_name)
        if not source:
            continue
        hard_deny_tokens = source.get("hard_deny_tokens", [])
        if not hard_deny_tokens:
            continue
        url_lower = url.lower()
        if any(isinstance(token, str) and token.lower() in url_lower for token in hard_deny_tokens):
            if status != "FAILED" or int(retry_count or 0) < MAX_RETRIES:
                conn.execute(
                    "UPDATE urls SET status = 'FAILED', retry_count = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?",
                    (MAX_RETRIES, url),
                )
                updated += 1

    return updated

def apply_seed_fallback_filters(conn: sqlite3.Connection) -> int:
    blocked_seed_urls = {
        seed
        for source in SOURCES
        for seed in [_canonicalize_url(source.get("url", ""))]
        if seed and not _allow_seed_fallback(source)
    }
    if not blocked_seed_urls:
        return 0

    cursor = conn.cursor()
    cursor.execute("SELECT url, status, retry_count FROM urls")
    rows = cursor.fetchall()

    updated = 0
    for url, status, retry_count in rows:
        canonical = _canonicalize_url(url) or url
        if canonical not in blocked_seed_urls:
            continue
        if status != "FAILED" or int(retry_count or 0) < MAX_RETRIES:
            conn.execute(
                "UPDATE urls SET status = 'FAILED', retry_count = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?",
                (MAX_RETRIES, url),
            )
            updated += 1
    return updated

def ensure_directories():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)

def init_db():
    """Initialize the SQLite KV store with WAL mode and robust tracking schemas."""
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS urls (
            url TEXT PRIMARY KEY,
            source_name TEXT,
            status TEXT DEFAULT 'PENDING',  -- PENDING, PROCESSING, COMPLETED, FAILED
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP,
            retry_count INTEGER DEFAULT 0,
            priority INTEGER DEFAULT 0
        )
    ''')
    try:
        # Just in case we are modifying an existing populated DB
        conn.execute('ALTER TABLE urls ADD COLUMN retry_count INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass # Column already exists
    try:
        conn.execute('ALTER TABLE urls ADD COLUMN priority INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass
    conn.execute('CREATE INDEX IF NOT EXISTS idx_urls_status_processed_at ON urls (status, processed_at)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_urls_status_priority ON urls (status, priority DESC, discovered_at ASC)')
    merged = normalize_existing_urls(conn)
    if merged > 0:
        print(f"Normalized URL table: merged {merged} duplicate/non-canonical rows.")
    filtered = apply_hard_deny_filters(conn)
    if filtered > 0:
        print(f"Applied hard-deny filters to {filtered} rows.")
    disabled_filtered = apply_disabled_source_filters(conn)
    if disabled_filtered > 0:
        print(f"Applied disabled-source filters to {disabled_filtered} rows.")
    seed_filtered = apply_seed_fallback_filters(conn)
    if seed_filtered > 0:
        print(f"Applied seed-fallback filters to {seed_filtered} rows.")
    if QUARANTINE_SUSPECT_OUTPUTS:
        moved = quarantine_suspect_outputs(conn)
        if moved > 0:
            print(f"Quarantined {moved} suspect markdown files to {REJECTED_DIR}.")
    conn.close()

def add_urls_to_db(urls: Sequence[Any], source_name: str) -> Tuple[int, int]:
    """Add newly mapped URLs to the database. Returns (added_count, priority_updates)."""
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    cursor = conn.cursor()
    added_count = 0
    updated_priority_count = 0

    for item in urls:
        if isinstance(item, tuple):
            url, priority = item
        else:
            url, priority = item, MIN_RELEVANCE_SCORE

        if not isinstance(url, str) or not url.startswith('http'):
            continue
        if not isinstance(priority, int):
            priority = MIN_RELEVANCE_SCORE

        try:
            cursor.execute('SELECT status, retry_count, priority FROM urls WHERE url = ?', (url,))
            existing = cursor.fetchone()
            if existing is None:
                conn.execute(
                    'INSERT INTO urls (url, source_name, status, priority) VALUES (?, ?, ?, ?)',
                    (url, source_name, 'PENDING', priority)
                )
                added_count += 1
                continue

            status, retry_count, existing_priority = existing
            should_update_priority = isinstance(existing_priority, int) and priority > existing_priority
            should_requeue_failed = status == 'FAILED' and retry_count < MAX_RETRIES

            if should_update_priority or should_requeue_failed:
                next_status = 'PENDING' if should_requeue_failed else status
                next_priority = priority if should_update_priority else existing_priority
                conn.execute(
                    'UPDATE urls SET source_name = ?, status = ?, priority = ? WHERE url = ?',
                    (source_name, next_status, next_priority, url)
                )
                if should_update_priority:
                    updated_priority_count += 1
        except sqlite3.Error as e:
            print(f"⚠️ Failed DB insert/update for {url}: {e}")
    conn.close()
    return added_count, updated_priority_count

def get_pending_chunk(chunk_size=100):
    """
    Fetch a chunk of PENDING URLs to scrape.
    Atomically marks them as PROCESSING to ensure multi-writer concurrent safety.
    Recovers URLs that were stuck in PROCESSING for over 2 hours.
    """
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    cursor = conn.cursor()
    transaction_started = False
    try:
        conn.execute('BEGIN IMMEDIATE') # Lock for claiming to avoid race conditions
        transaction_started = True
        cursor.execute('''
            SELECT url FROM urls 
            WHERE (status = 'PENDING' OR (status = 'PROCESSING' AND datetime(processed_at) < datetime('now', ?)))
              AND retry_count < ?
            ORDER BY priority DESC, discovered_at ASC
            LIMIT ?
        ''', (f'-{PROCESSING_STALE_HOURS} hour', MAX_RETRIES, chunk_size))
        rows = cursor.fetchall()
        urls = [r[0] for r in rows]
        
        if urls:
            placeholders = ','.join(['?'] * len(urls))
            cursor.execute(f'''
                UPDATE urls 
                SET status = 'PROCESSING', processed_at = CURRENT_TIMESTAMP 
                WHERE url IN ({placeholders})
            ''', urls)
            
        conn.execute('COMMIT')
        return urls
    except Exception:
        if transaction_started:
            conn.execute('ROLLBACK')
        raise
    finally:
        conn.close()

def mark_status(url, status, increment_retry=False):
    """Mark a URL status globally. Bumps retry_count if specified."""
    conn = sqlite3.connect(DB_PATH, isolation_level=None)
    conn.execute('PRAGMA journal_mode=WAL')
    if increment_retry:
        conn.execute(
            'UPDATE urls SET status = ?, processed_at = CURRENT_TIMESTAMP, retry_count = retry_count + 1 WHERE url = ?',
            (status, url)
        )
    else:
        conn.execute(
            'UPDATE urls SET status = ?, processed_at = CURRENT_TIMESTAMP WHERE url = ?',
            (status, url)
        )
    conn.close()

def handle_failure(url, error_context=""):
    """Handle extraction failures gracefully, checking retry upper limits."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT retry_count FROM urls WHERE url = ?', (url,))
    row = cursor.fetchone()
    conn.close()
    
    current_retries = row[0] if row else 0
    
    next_retry = current_retries + 1
    if next_retry >= MAX_RETRIES:
        mark_status(url, 'FAILED', increment_retry=True)
        print(f" ❌ PERMANENT FAIL [{next_retry}/{MAX_RETRIES}]: {url} ({error_context})")
    else:
        # Requeue for a future attempt
        mark_status(url, 'PENDING', increment_retry=True)
        print(f" ⚠️ REQUEUED [{next_retry}/{MAX_RETRIES}]: {url} ({error_context})")

@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=3, max=12))
def scrape_with_retry(app, url, only_main_content=True):
    """Scrape a URL with exponential backoff on failure (SDK level resiliency)."""
    return app.scrape(
        url, 
        formats=['markdown'], 
        only_main_content=only_main_content
    )

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=6))
def map_with_retry(app, seed_url: str, search_query: Optional[str] = None):
    kwargs: Dict[str, Any] = {"limit": MAP_LIMIT_PER_CALL}
    if search_query:
        kwargs["search"] = search_query
    return app.map(seed_url, **kwargs)

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=6))
def scrape_links_with_retry(app, url: str):
    return app.scrape(url, formats=["links"], only_main_content=False)

def _print_top_ranked_urls(ranked_urls: Sequence[Tuple[str, int]], sample_size: int = 5):
    if not ranked_urls:
        return
    print("  Top ranked candidate URLs:")
    for url, score in ranked_urls[:sample_size]:
        print(f"   - [{score:02d}] {url}")

def _source_map_budget(source: Dict[str, Any]) -> int:
    configured = source.get("max_urls")
    if isinstance(configured, int) and configured > 0:
        return configured
    return MAX_URLS_PER_SOURCE

def _seed_probe_metrics(
    source: Dict[str, Any],
    raw_links: Sequence[str],
    ranked_urls: Sequence[Tuple[str, int]],
    seed_score: int,
) -> Dict[str, Any]:
    seed_host = _source_host(source["url"])
    canonical_raw = {
        canonical
        for canonical in (_canonicalize_url(url) for url in raw_links)
        if canonical and _source_host(canonical) == seed_host
    }
    story_like_count = sum(1 for url, _score in ranked_urls if _is_story_like_url(url, source))
    high_signal_count = len(ranked_urls)
    high_signal_ratio = high_signal_count / len(canonical_raw) if canonical_raw else 0.0
    top_score = ranked_urls[0][1] if ranked_urls else 0
    top_window = ranked_urls[:10]
    avg_top_score = (
        sum(score for _url, score in top_window) / len(top_window)
        if top_window
        else 0.0
    )
    return {
        "raw_link_count": len(raw_links),
        "unique_host_link_count": len(canonical_raw),
        "high_signal_count": high_signal_count,
        "high_signal_ratio": high_signal_ratio,
        "story_like_count": story_like_count,
        "seed_score": seed_score,
        "top_score": top_score,
        "avg_top_score": avg_top_score,
    }

def probe_source_seed_quality(app, source: Dict[str, Any]) -> Dict[str, Any]:
    candidate_source = dict(source)
    canonical_seed = _canonicalize_url(source["url"])
    if canonical_seed:
        candidate_source["url"] = canonical_seed

    probe_links: List[str] = []
    probe_errors: List[str] = []
    network_like_failures = (
        "ConnectionError",
        "NameResolutionError",
        "Max retries exceeded",
        "Failed to establish a new connection",
    )

    if SEED_PROBE_MAP_ENABLED:
        try:
            map_result = map_with_retry(app, candidate_source["url"], search_query=None)
            probe_links.extend(_extract_links(map_result))
        except Exception as e:
            probe_errors.append(f"map: {e}")

    try:
        link_result = scrape_links_with_retry(app, candidate_source["url"])
        probe_links.extend(_extract_links(link_result))
    except Exception as e:
        probe_errors.append(f"links: {e}")

    if not probe_links and probe_errors:
        if all(any(marker in err for marker in network_like_failures) for err in probe_errors):
            raise FirecrawlConnectivityError(
                f"Connectivity to Firecrawl failed during seed probe for '{source['name']}'."
            )

    ranked_urls = _rank_candidate_urls(probe_links, candidate_source)
    seed_score = _score_url(candidate_source["url"], candidate_source)
    metrics = _seed_probe_metrics(candidate_source, probe_links, ranked_urls, seed_score)
    recommended, reason = _decide_seed_probe_recommendation(
        candidate_source,
        metrics["high_signal_count"],
        metrics["high_signal_ratio"],
        metrics["seed_score"],
    )
    metrics["recommended"] = recommended
    metrics["reason"] = reason
    metrics["errors"] = probe_errors
    metrics["top_urls"] = ranked_urls[:5]
    return metrics

def _print_seed_probe_result(source: Dict[str, Any], metrics: Dict[str, Any]):
    status = "PASS" if metrics.get("recommended") else "SKIP"
    print(
        f" Seed probe [{status}] {source['name']}: "
        f"raw={metrics['raw_link_count']}, unique_host={metrics['unique_host_link_count']}, "
        f"high_signal={metrics['high_signal_count']} ({metrics['high_signal_ratio']:.2%}), "
        f"seed_score={metrics['seed_score']}"
    )
    print(f"  ↳ Decision: {metrics.get('reason', '')}")
    errors = metrics.get("errors", [])
    for error in errors[:2]:
        print(f"  ↳ Probe error: {error}")

def _urls_table_exists(conn: sqlite3.Connection) -> bool:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='urls' LIMIT 1"
    )
    return cursor.fetchone() is not None

def _completed_pages_by_source() -> Dict[str, int]:
    if not DB_PATH.exists():
        return {}
    conn = sqlite3.connect(DB_PATH)
    try:
        if not _urls_table_exists(conn):
            return {}
        rows = conn.execute(
            "SELECT source_name, COUNT(*) FROM urls WHERE status='COMPLETED' GROUP BY source_name"
        ).fetchall()
        return {source_name: int(count) for source_name, count in rows if isinstance(source_name, str)}
    finally:
        conn.close()

def _completed_cases_by_source() -> Dict[str, int]:
    if not DB_PATH.exists():
        return {}
    conn = sqlite3.connect(DB_PATH)
    try:
        if not _urls_table_exists(conn):
            return {}
        rows = conn.execute(
            "SELECT url, source_name FROM urls WHERE status='COMPLETED'"
        ).fetchall()
    finally:
        conn.close()

    source_cases: Dict[str, int] = {}
    processed_dir = BASE_DIR / "public_data" / "processed"
    for url, source_name in rows:
        if not isinstance(url, str) or not isinstance(source_name, str):
            continue
        output_path = processed_dir / f"{hashlib.md5(url.encode('utf-8')).hexdigest()}.json"
        if not output_path.exists():
            continue
        try:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        cases = payload.get("cases", []) if isinstance(payload, dict) else []
        if isinstance(cases, list):
            source_cases[source_name] = source_cases.get(source_name, 0) + len(cases)
    return source_cases

def get_condition_coverage() -> Dict[str, Dict[str, Any]]:
    source_pages = _completed_pages_by_source()
    source_cases = _completed_cases_by_source()
    source_index = {source.get("name"): source for source in SOURCES}

    coverage: Dict[str, Dict[str, Any]] = {}
    for condition_id, payload in CONDITION_TARGETS.items():
        target_pages = int(payload.get("target_completed_pages", 0))
        target_cases = int(payload.get("target_cases", 0))
        coverage[condition_id] = {
            "label": payload.get("label", condition_id),
            "target_completed_pages": target_pages,
            "target_cases": target_cases,
            "completed_pages": 0,
            "completed_cases": 0,
        }

    for source_name, page_count in source_pages.items():
        source = source_index.get(source_name, {})
        for condition_id in _source_condition_tags(source):
            if condition_id not in coverage:
                coverage[condition_id] = {
                    "label": condition_id,
                    "target_completed_pages": 0,
                    "target_cases": 0,
                    "completed_pages": 0,
                    "completed_cases": 0,
                }
            coverage[condition_id]["completed_pages"] += int(page_count)

    for source_name, case_count in source_cases.items():
        source = source_index.get(source_name, {})
        for condition_id in _source_condition_tags(source):
            if condition_id not in coverage:
                coverage[condition_id] = {
                    "label": condition_id,
                    "target_completed_pages": 0,
                    "target_cases": 0,
                    "completed_pages": 0,
                    "completed_cases": 0,
                }
            coverage[condition_id]["completed_cases"] += int(case_count)

    for condition_id, payload in coverage.items():
        payload["pages_deficit"] = max(0, payload["target_completed_pages"] - payload["completed_pages"])
        payload["cases_deficit"] = max(0, payload["target_cases"] - payload["completed_cases"])
    return coverage

def _print_condition_coverage_summary(coverage: Dict[str, Dict[str, Any]], max_rows: int = 20):
    print("--- Condition Coverage ---")
    ranked = sorted(
        coverage.items(),
        key=lambda item: (-item[1]["pages_deficit"], item[0]),
    )
    for condition_id, payload in ranked[:max_rows]:
        print(
            f" {condition_id}: pages {payload['completed_pages']}/{payload['target_completed_pages']} "
            f"(deficit {payload['pages_deficit']}), cases {payload['completed_cases']}/{payload['target_cases']} "
            f"(deficit {payload['cases_deficit']})"
        )

def _source_deficit_score(source: Dict[str, Any], coverage: Dict[str, Dict[str, Any]]) -> int:
    tags = _source_condition_tags(source)
    if not tags:
        return 0
    return max((coverage.get(tag, {}).get("pages_deficit", 0) for tag in tags), default=0)

def _filter_sources_by_condition_quotas(sources: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    coverage = get_condition_coverage()
    selected: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for source in sources:
        tags = _source_condition_tags(source)
        if not tags:
            selected.append(source)
            continue
        if any(coverage.get(tag, {}).get("pages_deficit", 0) > 0 for tag in tags):
            selected.append(source)
        else:
            skipped.append(source)

    selected.sort(key=lambda source: (-_source_deficit_score(source, coverage), source.get("name", "")))
    return selected, skipped, coverage

def _condition_keywords(condition_id: str) -> List[str]:
    payload = CONDITION_TARGETS.get(condition_id, {})
    keywords = [
        keyword.strip().lower()
        for keyword in payload.get("url_keywords", [])
        if isinstance(keyword, str) and keyword.strip()
    ]
    for axis in payload.get("suffering_axes", []):
        if not isinstance(axis, str):
            continue
        axis_clean = axis.strip().lower()
        if not axis_clean:
            continue
        keywords.append(axis_clean)
    label = payload.get("label")
    if isinstance(label, str):
        keywords.extend(token.lower() for token in re.findall(r"[a-z0-9]+", label) if len(token) > 2)
    return list(dict.fromkeys(keywords))

def _condition_search_queries(condition_id: str) -> List[str]:
    payload = CONDITION_TARGETS.get(condition_id, {})
    label = payload.get("label", condition_id)
    queries = [
        query.strip()
        for query in payload.get("seed_queries", [])
        if isinstance(query, str) and query.strip()
    ]

    suffering_axes = [
        axis.strip()
        for axis in payload.get("suffering_axes", [])
        if isinstance(axis, str) and axis.strip()
    ][:3]
    for axis in suffering_axes:
        queries.append(f"{label} {axis} patient forum")
        queries.append(f"{label} {axis} lived experience")

    keywords = _condition_keywords(condition_id)
    for token in keywords[:4]:
        if " " in token:
            continue
        queries.append(f"{label} {token} patient story")

    if not queries:
        queries = [f"{label} patient stories forum"]
    return list(dict.fromkeys(queries))[:8]

def _condition_seed_hosts(condition_id: str) -> List[str]:
    payload = CONDITION_TARGETS.get(condition_id, {})
    hosts: List[str] = []
    for domain in payload.get("seed_domains", []):
        if isinstance(domain, str) and domain.strip():
            hosts.append(domain.strip().lower())

    for source in SOURCES:
        if condition_id in _source_condition_tags(source):
            host = _source_host(source.get("url", ""))
            if host:
                hosts.append(host)

    for seed_url in DISEASE_LIST_SEED_URLS:
        host = _source_host(seed_url)
        if host:
            hosts.append(host)
    return list(dict.fromkeys(hosts))

def _host_matches_seed_hosts(host: str, seed_hosts: Iterable[str]) -> bool:
    normalized_host = host.strip().lower()
    if not normalized_host:
        return False
    for seed_host in seed_hosts:
        normalized_seed = seed_host.strip().lower()
        if not normalized_seed:
            continue
        if normalized_host == normalized_seed or normalized_host.endswith(f".{normalized_seed}"):
            return True
    return False

def _looks_like_discovery_junk(url: str) -> bool:
    lower = url.lower()
    host = _source_host(url)
    if any(marker in host for marker in DISCOVERY_JUNK_HOST_MARKERS):
        return True
    if any(token in lower for token in DISCOVERY_JUNK_URL_TOKENS):
        return True
    return False

def _looks_like_seed_listing_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    query = parsed.query.lower()
    return (
        any(token in path for token in ("/forums/", "/forum/", "/categories/", "/tag/", "/topics", "/conditions/"))
        or ("default.aspx" in path and "f=" in query and "m=" not in query)
    )

def _http_get_text(url: str, timeout: Optional[float] = None) -> str:
    resolved_timeout = timeout if timeout is not None else DISCOVERY_HTTP_TIMEOUT_SECONDS
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; SufferpediaSeedBot/1.0)",
            "Accept": "text/html,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urlopen(request, timeout=resolved_timeout) as response:
        return response.read().decode("utf-8", errors="ignore")

def _extract_href_links_from_html(html_text: str, base_url: str) -> List[str]:
    links: List[str] = []
    for href in re.findall(r'href=[\"\']([^\"\']+)[\"\']', html_text, flags=re.IGNORECASE):
        absolute = urljoin(base_url, href.strip())
        canonical = _canonicalize_url(absolute)
        if canonical:
            links.append(canonical)
    return links

def _extract_anchor_targets_from_html(html_text: str, base_url: str) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    for href, text in re.findall(
        r'<a[^>]+href=[\"\']([^\"\']+)[\"\'][^>]*>(.*?)</a>',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        absolute = urljoin(base_url, href.strip())
        canonical = _canonicalize_url(absolute)
        if not canonical:
            continue
        anchor_text = re.sub(r"<[^>]+>", " ", text)
        anchor_text = re.sub(r"\s+", " ", anchor_text).strip().lower()
        results.append((canonical, anchor_text))
    return results

def _discover_search_urls(condition_id: str) -> List[str]:
    queries = _condition_search_queries(condition_id)
    keywords = _condition_keywords(condition_id)
    known_hosts = _condition_seed_hosts(condition_id)
    discovered: List[str] = []
    for query in queries[:4]:
        search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
        try:
            html_text = _http_get_text(search_url, timeout=max(5.0, DISCOVERY_HTTP_TIMEOUT_SECONDS))
        except (HTTPError, URLError, TimeoutError, socket.timeout, OSError, ValueError):
            continue

        hits: List[str] = []
        for encoded in re.findall(r"uddg=([^&\"']+)", html_text):
            decoded = unquote(encoded)
            if decoded.startswith("http"):
                hits.append(decoded)
        for direct in re.findall(r'href=[\"\'](https?://[^\"\']+)[\"\']', html_text):
            hits.append(direct)

        seen_query: set[str] = set()
        for url in hits:
            canonical = _canonicalize_url(url)
            if not canonical or canonical in seen_query:
                continue
            if _looks_like_discovery_junk(canonical):
                continue
            host = _source_host(canonical)
            keyword_match = any(keyword in canonical.lower() for keyword in keywords)
            if not _host_matches_seed_hosts(host, known_hosts) and not (_looks_like_seed_listing_url(canonical) and keyword_match):
                continue
            seen_query.add(canonical)
            discovered.append(canonical)
            if len(seen_query) >= DISCOVERY_SEARCH_RESULTS_PER_QUERY:
                break
    return discovered

def _discover_sitemap_urls(condition_id: str) -> List[str]:
    domains = _condition_seed_hosts(condition_id)[:DISCOVERY_MAX_SEED_HOSTS_PER_CONDITION]
    keywords = _condition_keywords(condition_id)
    discovered: List[str] = []

    for domain in domains:
        queue = [
            f"https://{domain}/sitemap.xml",
            f"https://{domain}/sitemap_index.xml",
        ]
        visited_xml: set[str] = set()
        while queue and len(discovered) < DISCOVERY_SITEMAP_URL_LIMIT:
            sitemap_url = queue.pop(0)
            if sitemap_url in visited_xml:
                continue
            visited_xml.add(sitemap_url)
            try:
                xml_text = _http_get_text(sitemap_url, timeout=max(4.0, DISCOVERY_HTTP_TIMEOUT_SECONDS))
            except (HTTPError, URLError, TimeoutError, socket.timeout, OSError, ValueError):
                continue

            for loc in re.findall(r"<loc>(.*?)</loc>", xml_text, flags=re.IGNORECASE):
                location = loc.strip()
                canonical = _canonicalize_url(location)
                if not canonical:
                    continue
                if (
                    canonical.lower().endswith(".xml")
                    and canonical not in visited_xml
                    and len(visited_xml) < DISCOVERY_SITEMAP_MAX_XMLS_PER_DOMAIN
                ):
                    queue.append(canonical)
                    continue

                candidate_lower = canonical.lower()
                if _looks_like_discovery_junk(canonical):
                    continue
                if _looks_like_seed_listing_url(canonical) or any(keyword in candidate_lower for keyword in keywords):
                    discovered.append(canonical)
                    if len(discovered) >= DISCOVERY_SITEMAP_URL_LIMIT:
                        break
    return discovered

def _discover_disease_list_urls(app: FirecrawlApp, condition_id: str) -> List[str]:
    keywords = _condition_keywords(condition_id)
    known_hosts = _condition_seed_hosts(condition_id)
    discovered: List[str] = []
    for list_seed in DISEASE_LIST_SEED_URLS[:8]:
        canonical_list_seed = _canonicalize_url(list_seed)
        if not canonical_list_seed:
            continue
        links: List[str] = []
        anchor_matches: Dict[str, bool] = {}
        try:
            scrape_links = scrape_links_with_retry(app, canonical_list_seed)
            links = _extract_links(scrape_links)
        except Exception:
            links = []

        try:
            html_text = _http_get_text(canonical_list_seed, timeout=max(4.0, DISCOVERY_HTTP_TIMEOUT_SECONDS))
            if not links:
                links = _extract_href_links_from_html(html_text, canonical_list_seed)
            for candidate_url, anchor_text in _extract_anchor_targets_from_html(html_text, canonical_list_seed):
                if any(keyword in anchor_text for keyword in keywords):
                    anchor_matches[candidate_url] = True
        except (HTTPError, URLError, TimeoutError, socket.timeout, OSError, ValueError):
            if not links:
                continue

        list_host = _source_host(canonical_list_seed)
        for link in links:
            canonical = _canonicalize_url(link)
            if not canonical:
                continue
            if _looks_like_discovery_junk(canonical):
                continue
            lower = canonical.lower()
            host = _source_host(canonical)
            trusted_host = _host_matches_seed_hosts(host, known_hosts)
            same_host_listing = _source_host(canonical) == list_host and _looks_like_seed_listing_url(canonical)
            keyword_match = any(keyword in lower for keyword in keywords)
            anchor_match = anchor_matches.get(canonical, False)
            if not trusted_host and not same_host_listing:
                continue
            if same_host_listing and not (keyword_match or anchor_match):
                continue
            if same_host_listing or keyword_match or anchor_match:
                discovered.append(canonical)
    return discovered

def _discover_linkgraph_urls(app: FirecrawlApp, condition_id: str) -> List[str]:
    discovered: List[str] = []
    keywords = _condition_keywords(condition_id)
    known_hosts = _condition_seed_hosts(condition_id)
    tagged_sources = [
        source for source in SOURCES
        if condition_id in _source_condition_tags(source) and _source_is_enabled(source)
    ]
    for source in tagged_sources[:8]:
        source_host = _source_host(source["url"])
        try:
            scrape_links = scrape_links_with_retry(app, source["url"])
            links = _extract_links(scrape_links)
        except Exception:
            continue
        for link in links:
            canonical = _canonicalize_url(link)
            if not canonical:
                continue
            if _looks_like_discovery_junk(canonical):
                continue
            host = _source_host(canonical)
            if host != source_host and not _host_matches_seed_hosts(host, known_hosts):
                continue
            lower = canonical.lower()
            if _looks_like_seed_listing_url(canonical) or any(keyword in lower for keyword in keywords):
                discovered.append(canonical)
    return discovered

def _build_discovered_source(condition_id: str, url: str) -> Dict[str, Any]:
    payload = CONDITION_TARGETS.get(condition_id, {})
    label = payload.get("label", condition_id)
    canonical = _canonicalize_url(url) or url
    parsed = urlparse(canonical)
    queries = [
        query.strip()
        for query in payload.get("seed_queries", [])
        if isinstance(query, str) and query.strip()
    ][:3]
    if not queries:
        queries = [label]

    keywords = _condition_keywords(condition_id)
    source: Dict[str, Any] = {
        "name": f"Discovered - {label} - {parsed.netloc}{parsed.path[:48]}",
        "url": canonical,
        "queries": queries,
        "allow_tokens": keywords[:6] + ["patient", "story", "experience"],
        "prefer_tokens": ["/threads/", "/topic/", "/t/", "/story/", "/stories/"],
        "condition_tags": [condition_id],
        "max_urls": 120,
        "seed_probe_min_high_signal_links": max(2, SEED_PROBE_MIN_HIGH_SIGNAL_LINKS - 1),
    }

    path_lower = parsed.path.lower()
    query_lower = parsed.query.lower()
    if "community.patient.info" in parsed.netloc.lower():
        source["prefer_tokens"] = ["/t/"]
        source["allow_tokens"] = ["community.patient.info", "/t/", "/tag/"] + keywords[:5]
        source["require_story_like_urls"] = True
        source["allow_seed_fallback"] = False
    if "healingwell.com" in parsed.netloc.lower() and "default.aspx" in path_lower and "f=" in query_lower:
        forum_id = ""
        for key, value in parse_qsl(parsed.query, keep_blank_values=True):
            if key.lower() == "f" and value.strip():
                forum_id = value.strip()
                break
        required_tokens = [f"f={forum_id}"] if forum_id else []
        source["prefer_tokens"] = ["m="]
        source["allow_tokens"] = ["default.aspx", "m="] + required_tokens + keywords[:5]
        source["required_tokens"] = required_tokens
        source["deny_tokens"] = ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"]
        source["hard_deny_tokens"] = ["profile.aspx", "login.aspx", "register.aspx", "post.aspx"]
        source["require_story_like_urls"] = True
        source["allow_seed_fallback"] = False
    return source

def discover_seed_candidates(app: FirecrawlApp, condition_ids: Sequence[str]) -> List[Dict[str, Any]]:
    dedup: Dict[str, Dict[str, Any]] = {}
    for condition_id in condition_ids:
        print(f" Discovering condition seeds: {condition_id}...")
        search_urls = _discover_search_urls(condition_id)
        sitemap_urls = _discover_sitemap_urls(condition_id)
        registry_list_urls = _discover_disease_list_urls(app, condition_id)
        linkgraph_urls = _discover_linkgraph_urls(app, condition_id)
        combined = search_urls + sitemap_urls + registry_list_urls + linkgraph_urls

        scored: List[Tuple[str, int]] = []
        keywords = _condition_keywords(condition_id)
        known_hosts = set(_condition_seed_hosts(condition_id))
        for url in combined:
            canonical = _canonicalize_url(url)
            if not canonical:
                continue
            if _looks_like_discovery_junk(canonical):
                continue
            lower = canonical.lower()
            host = _source_host(canonical)
            score = 0
            if _looks_like_seed_listing_url(canonical):
                score += 5
            if any(keyword in lower for keyword in keywords):
                score += 4
            if any(token in lower for token in ("/story/", "/stories/", "/forum/", "/forums/", "/tag/", "/topic/")):
                score += 3
            if _host_matches_seed_hosts(host, known_hosts):
                score += 4
            if score < DISCOVERY_MIN_CANDIDATE_SCORE:
                continue
            scored.append((canonical, score))

        scored.sort(key=lambda item: (-item[1], item[0]))
        selected_urls: List[str] = []
        selected_hosts: Dict[str, int] = defaultdict(int)
        seen_local: set[str] = set()
        host_cap = max(2, max(1, DISCOVERY_MAX_CANDIDATES_PER_CONDITION // 3))

        for canonical, _score in scored:
            if canonical in seen_local:
                continue
            if any(_canonicalize_url(source["url"]) == canonical for source in SOURCES):
                continue
            host = _source_host(canonical)
            if selected_hosts[host] >= host_cap:
                continue
            seen_local.add(canonical)
            selected_hosts[host] += 1
            selected_urls.append(canonical)
            if len(selected_urls) >= DISCOVERY_MAX_CANDIDATES_PER_CONDITION:
                break

        if len(selected_urls) < DISCOVERY_MAX_CANDIDATES_PER_CONDITION:
            for canonical, _score in scored:
                if canonical in seen_local:
                    continue
                if any(_canonicalize_url(source["url"]) == canonical for source in SOURCES):
                    continue
                seen_local.add(canonical)
                selected_urls.append(canonical)
                if len(selected_urls) >= DISCOVERY_MAX_CANDIDATES_PER_CONDITION:
                    break

        for candidate_url in selected_urls:
            key = f"{condition_id}|{candidate_url}"
            dedup[key] = _build_discovered_source(condition_id, candidate_url)
        print(
            f"  ↳ discovery candidates: search={len(search_urls)}, sitemap={len(sitemap_urls)}, "
            f"disease_list={len(registry_list_urls)}, linkgraph={len(linkgraph_urls)}, selected={len(selected_urls)}"
        )

    return list(dedup.values())

def _save_discovered_seed_candidates(candidates: Sequence[Dict[str, Any]]):
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "candidate_count": len(candidates),
        "candidates": list(candidates),
    }
    DISCOVERED_SEEDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    DISCOVERED_SEEDS_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

def map_single_source(app, source: Dict[str, Any]) -> Tuple[int, int]:
    seed_url = source["url"]
    name = source["name"]
    queries = _source_queries(source)
    source_map_delay = _source_map_delay_seconds(source)

    discovered: List[str] = [seed_url]
    total_raw = 0
    failed_calls = 0
    failure_messages: List[str] = []
    zero_link_streak = 0

    print(f"Mapping source: {name} | Root URL: {seed_url}")
    for idx, query in enumerate(queries):
        label = "seed scan" if query is None else f"query='{query}'"
        try:
            map_result = map_with_retry(app, seed_url, search_query=query)
            links = _extract_links(map_result)
            total_raw += len(links)
            discovered.extend(links)
            print(f" ↳ {label}: discovered {len(links)} links.")
            if len(links) == 0:
                zero_link_streak += 1
            else:
                zero_link_streak = 0
        except Exception as e:
            failed_calls += 1
            failure_messages.append(str(e))
            print(f" ↳ {label}: failed ({e})")
            zero_link_streak = 0

        if (
            EARLY_STOP_MAP_ZERO_STREAK > 0
            and total_raw == 0
            and zero_link_streak >= EARLY_STOP_MAP_ZERO_STREAK
            and idx < len(queries) - 1
        ):
            print(
                " ↳ map returned zero links repeatedly; "
                "stopping map queries early and switching to fallback discovery."
            )
            break

        if idx < len(queries) - 1:
            print(f"   Sleeping {source_map_delay:.1f}s to respect API limits...")
            time.sleep(source_map_delay)

    if total_raw == 0:
        try:
            scrape_links_result = scrape_links_with_retry(app, seed_url)
            fallback_links = _extract_links(scrape_links_result)
            fallback_ranked = _rank_candidate_urls(fallback_links, source)
            if len(fallback_ranked) >= MIN_FALLBACK_HIGH_SIGNAL_LINKS:
                discovered.extend([url for url, _score in fallback_ranked])
                total_raw += len(fallback_links)
                print(
                    f" ↳ scrape-link fallback: discovered {len(fallback_links)} links, "
                    f"accepted {len(fallback_ranked)} high-signal links."
                )
            else:
                print(
                    f" ↳ scrape-link fallback discovered {len(fallback_links)} links but only "
                    f"{len(fallback_ranked)} passed relevance; ignoring fallback set."
                )
        except Exception as e:
            print(f" ↳ scrape-link fallback failed ({e})")

    ranked_urls = _rank_candidate_urls(discovered, source)
    budget = _source_map_budget(source)
    selected_urls = ranked_urls[:budget]
    source_min_score = _source_min_score(source)

    if total_raw == 0 and failed_calls == len(queries):
        if failure_messages and all(_looks_like_auth_error(msg) for msg in failure_messages):
            raise FirecrawlAuthenticationError(
                f"Authentication to Firecrawl failed while mapping '{name}'."
            )
        network_like_failures = (
            "ConnectionError",
            "NameResolutionError",
            "Max retries exceeded",
            "Failed to establish a new connection",
        )
        if failure_messages and all(any(marker in msg for marker in network_like_failures) for msg in failure_messages):
            raise FirecrawlConnectivityError(
                f"Connectivity to Firecrawl failed while mapping '{name}'."
            )

    if not selected_urls:
        canonical_seed = _canonicalize_url(seed_url)
        if canonical_seed and _allow_seed_fallback(source):
            selected_urls = [(canonical_seed, MIN_RELEVANCE_SCORE)]
            print(" ↳ No URLs passed relevance filter; keeping canonical seed as fallback.")
        else:
            print(" ↳ No URLs passed relevance filter; skipping canonical seed fallback for forum-like source.")

    added, updated = add_urls_to_db(selected_urls, name)

    print(
        f" ↳ Kept {len(selected_urls)} high-signal URLs "
        f"(from {total_raw} raw links, threshold={source_min_score})."
    )
    print(f" ↳ DB updates: {added} new URLs, {updated} priority upgrades.")
    _print_top_ranked_urls(selected_urls)
    return added, len(selected_urls)

def _probe_filter_sources(app: FirecrawlApp, sources: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    for source in sources:
        metrics = probe_source_seed_quality(app, source)
        _print_seed_probe_result(source, metrics)
        if metrics.get("recommended"):
            selected.append(source)
    return selected

def audit_seed_sources(app, include_discovery: bool = False, condition_ids: Optional[Sequence[str]] = None):
    configured_sources = SOURCES[:MAX_SOURCES] if MAX_SOURCES > 0 else SOURCES
    coverage = get_condition_coverage()
    print("--- Seed Source Audit ---")
    _print_condition_coverage_summary(coverage)
    for source in configured_sources:
        if not _source_is_enabled(source):
            reason = source.get("disabled_reason")
            if isinstance(reason, str) and reason.strip():
                print(f" Seed probe [SKIP] {source['name']}: disabled ({reason.strip()})")
            else:
                print(f" Seed probe [SKIP] {source['name']}: disabled")
            continue

        metrics = probe_source_seed_quality(app, source)
        _print_seed_probe_result(source, metrics)
        for url, score in metrics.get("top_urls", [])[:3]:
            print(f"  ↳ Top candidate [{score:02d}] {url}")

    if not include_discovery:
        return

    target_conditions = [
        condition_id for condition_id, payload in coverage.items()
        if payload.get("pages_deficit", 0) > 0
    ]
    if condition_ids:
        target_conditions = [condition_id for condition_id in condition_ids if condition_id in CONDITION_TARGETS]
    if not target_conditions:
        target_conditions = sorted(CONDITION_TARGETS.keys())

    discovered = discover_seed_candidates(app, target_conditions)
    _save_discovered_seed_candidates(discovered)
    print(
        f"--- Discovered Seed Candidates ({len(discovered)}) "
        f"saved to {DISCOVERED_SEEDS_PATH} ---"
    )
    for source in discovered:
        metrics = probe_source_seed_quality(app, source)
        _print_seed_probe_result(source, metrics)
        for url, score in metrics.get("top_urls", [])[:2]:
            print(f"  ↳ Top candidate [{score:02d}] {url}")

def map_sources(app):
    """Phase 1: Discover URLs deeper in the index/seed forums."""
    print("--- Phase 1: Mapping Target Forums ---")
    total_added = 0
    total_selected = 0
    configured_sources = SOURCES[:MAX_SOURCES] if MAX_SOURCES > 0 else SOURCES
    statically_enabled_sources = [source for source in configured_sources if _source_is_enabled(source)]
    disabled_sources = [source for source in configured_sources if not _source_is_enabled(source)]

    for source in disabled_sources:
        reason = source.get("disabled_reason")
        if isinstance(reason, str) and reason.strip():
            print(f"Skipping disabled source: {source['name']} ({reason.strip()})")
        else:
            print(f"Skipping disabled source: {source['name']}")

    if not statically_enabled_sources:
        print("No enabled sources configured for mapping.")
        return

    active_sources, quota_skipped, coverage = _filter_sources_by_condition_quotas(statically_enabled_sources)
    _print_condition_coverage_summary(coverage)
    for source in quota_skipped:
        print(f"Skipping source (condition quota met): {source['name']}")

    if not active_sources:
        print("No enabled sources need additional coverage right now.")
        return

    if SEED_PROBE_ENABLED:
        print(
            "Running seed probe gate "
            f"(min_high_signal={SEED_PROBE_MIN_HIGH_SIGNAL_LINKS}, "
            f"min_ratio={SEED_PROBE_MIN_HIGH_SIGNAL_RATIO:.2f})..."
        )
        active_sources = _probe_filter_sources(app, active_sources)

    if not active_sources:
        print("No sources passed seed probe. Mapping skipped.")
        return

    if DISCOVERY_ENABLED:
        unmet_conditions = [
            condition_id for condition_id, payload in coverage.items()
            if payload.get("pages_deficit", 0) > 0
        ]
        discovered_sources = discover_seed_candidates(app, unmet_conditions)
        if discovered_sources:
            _save_discovered_seed_candidates(discovered_sources)
            print(
                f"Running discovery seed audit for {len(discovered_sources)} candidates "
                f"(saved to {DISCOVERED_SEEDS_PATH})..."
            )
            discovered_selected = (
                _probe_filter_sources(app, discovered_sources)
                if SEED_PROBE_ENABLED
                else list(discovered_sources)
            )
            active_sources.extend(discovered_selected)

    deduped_sources: List[Dict[str, Any]] = []
    seen_seed_urls: set[str] = set()
    for source in active_sources:
        canonical = _canonicalize_url(source.get("url", "")) or source.get("url", "")
        if not canonical or canonical in seen_seed_urls:
            continue
        seen_seed_urls.add(canonical)
        deduped_sources.append(source)
    active_sources = deduped_sources

    for index, source in enumerate(active_sources):
        added, selected = map_single_source(app, source)
        total_added += added
        total_selected += selected
        if index < len(active_sources) - 1:
            source_delay = _source_map_delay_seconds(source)
            print(f"Sleeping {source_delay:.1f}s before next source...")
            time.sleep(source_delay)

    print(
        f"Mapping complete: selected {total_selected} high-signal URLs "
        f"across sources ({total_added} new DB rows)."
    )

def process_chunk(app, url):
    """Process a single URL inside a ThreadPoolExecutor."""
    output_path = _output_path_for_url(url)
    
    # Check filesystem cache just in case DB is out of sync or we are restarting
    if output_path.exists() and output_path.stat().st_size > 0:
        cached_content = output_path.read_text(encoding='utf-8', errors='ignore')
        cached_source_url = _extract_source_url_from_markdown(cached_content) or url
        if not _is_suspect_markdown_content(cached_content, cached_source_url):
            mark_status(url, 'COMPLETED')
            return True

        quarantined_path = _move_to_rejected(output_path)
        if quarantined_path is not None:
            print(f" ⚠️ Quarantined suspect cached file for {url}: {quarantined_path.name}")
        
    try:
        scrape_result = scrape_with_retry(app, url, only_main_content=True)
        markdown_content = _extract_markdown(scrape_result).strip()
        min_chars = _min_markdown_chars_for_url(url)

        if len(markdown_content) < min_chars:
            fallback_result = scrape_with_retry(app, url, only_main_content=False)
            fallback_markdown = _extract_markdown(fallback_result).strip()
            if len(fallback_markdown) > len(markdown_content):
                markdown_content = fallback_markdown

        if markdown_content and len(markdown_content) >= min_chars:
            if _is_suspect_markdown_content(markdown_content, url):
                handle_failure(url, error_context="Low-signal/interstitial/auth/error content detected.")
                return False
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"<!-- SOURCE_URL: {url} -->\n\n{markdown_content}\n")
            mark_status(url, 'COMPLETED')
            return True
        else:
            handle_failure(
                url,
                error_context=f"Insufficient markdown extracted ({len(markdown_content)} chars, needed {min_chars}).",
            )
            return False
    except Exception as e:
        handle_failure(url, error_context=str(e))
        return False

def maybe_run_parser_continuous(force: bool = False):
    global _LAST_PARSER_RUN_TS, _PARSER_WARNED_MISSING_KEY
    if not PARSER_CONTINUOUS_ENABLED:
        return

    if not os.environ.get("GEMINI_API_KEY"):
        if not _PARSER_WARNED_MISSING_KEY:
            print("Parser continuous mode skipped: GEMINI_API_KEY is not set.")
            _PARSER_WARNED_MISSING_KEY = True
        return

    now = time.time()
    if not force and _LAST_PARSER_RUN_TS > 0 and (now - _LAST_PARSER_RUN_TS) < PARSER_MIN_INTERVAL_SECONDS:
        return

    parser_script = BASE_DIR / "scripts" / "2_parse.py"
    if not parser_script.exists():
        print(f"Parser continuous mode skipped: missing parser script at {parser_script}")
        return

    parser_cmd: List[str]
    uv_bin = shutil.which("uv")
    if uv_bin:
        parser_cmd = [uv_bin, "run", "python", str(parser_script)]
    else:
        parser_cmd = [sys.executable, str(parser_script)]

    print("Starting continuous parser pass for newly scraped files...")
    try:
        result = subprocess.run(
            parser_cmd,
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as e:
        print(f"Continuous parser run failed to start: {e}")
        return

    _LAST_PARSER_RUN_TS = now
    if result.returncode != 0:
        print(f"Continuous parser exited with code {result.returncode}.")
    stdout_lines = [line for line in result.stdout.splitlines() if line.strip()]
    stderr_lines = [line for line in result.stderr.splitlines() if line.strip()]
    for line in stdout_lines[-5:]:
        print(f" [parser] {line}")
    for line in stderr_lines[-3:]:
        print(f" [parser:stderr] {line}")

def scrape_phase(app, chunk_size=100, max_workers=10):
    """Phase 2: Scrape PENDING URLs concurrently using a ThreadPoolExecutor."""
    print(f"\n--- Phase 2: Scraping URLs (Chunk Size: {chunk_size}, Workers: {max_workers}) ---")
    
    any_successful_scrapes = False
    while True:
        pending_urls = get_pending_chunk(chunk_size)
        if not pending_urls:
            print("No PENDING URLs left in the database. Scrape Phase finished.")
            break
            
        print(f"\nProcessing chunk of {len(pending_urls)} URLs...")
        successful_in_chunk = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {executor.submit(process_chunk, app, url): url for url in pending_urls}
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    success = future.result()
                    if success:
                        print(f" ✅ Scraped: {url}")
                        successful_in_chunk += 1
                except Exception as e:
                    handle_failure(url, error_context=f"Thread exception: {e}")
                    print(f" ⚠️ Thread exception for {url}: {e}")

        if successful_in_chunk > 0:
            any_successful_scrapes = True
            maybe_run_parser_continuous(force=False)
        print(f"Chunk completed. Backing off slightly before checking next batch...")
        time.sleep(CHUNK_DELAY_SECONDS)

    if any_successful_scrapes:
        maybe_run_parser_continuous(force=True)

def _maybe_load_env_file(env_path: Path):
    if not env_path.exists():
        return
    try:
        lines = env_path.read_text(encoding='utf-8').splitlines()
    except Exception as e:
        print(f"Warning: failed to read {env_path}: {e}")
        return

    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value

def audit_crawl_outputs():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT status, COUNT(*) FROM urls GROUP BY status ORDER BY status")
    status_counts = cursor.fetchall()
    conn.close()

    files = sorted(DATA_DIR.glob("*.md"))
    suspect_files: List[str] = []
    for file_path in files:
        content = file_path.read_text(encoding='utf-8', errors='ignore')
        source_url = _extract_source_url_from_markdown(content)
        if _is_suspect_markdown_content(content, source_url):
            suspect_files.append(file_path.name)

    print("\n--- Crawl Audit ---")
    if status_counts:
        print("DB status counts:")
        for status, count in status_counts:
            print(f" - {status}: {count}")
    else:
        print("DB status counts: (none)")
    print(f"Scraped markdown files: {len(files)}")
    print(f"Suspect low-signal files: {len(suspect_files)}")
    for file_name in suspect_files[:20]:
        print(f" - {file_name}")
    rejected_files = sorted(REJECTED_DIR.glob("*.md")) if REJECTED_DIR.exists() else []
    print(f"Quarantined markdown files: {len(rejected_files)}")
    coverage = get_condition_coverage()
    _print_condition_coverage_summary(coverage, max_rows=40)

def preflight_firecrawl_connectivity(app):
    probe_source = next((source for source in SOURCES if _source_is_enabled(source)), None)
    if probe_source is None:
        return
    probe_url = probe_source["url"]
    try:
        app.map(probe_url, limit=1)
    except Exception as e:
        message = str(e)
        if _looks_like_auth_error(message):
            raise FirecrawlAuthenticationError(
                f"Firecrawl authentication failed for {probe_url}: {e}"
            ) from e
        raise FirecrawlConnectivityError(
            f"Firecrawl preflight failed for {probe_url}: {e}"
        ) from e

def main():
    ensure_directories()
    _maybe_load_env_file(BASE_DIR / ".env")
    load_source_registry()
    init_db()

    if "--audit-only" in sys.argv:
        audit_crawl_outputs()
        return

    api_key = os.environ.get("FIRECRAWL_API_KEY")
    if not api_key:
        print("Error: FIRECRAWL_API_KEY environment variable not set. Please export it.")
        return
    
    request_timeout = float(os.environ.get("CRAWL_REQUEST_TIMEOUT", "20"))
    sdk_retries = int(os.environ.get("CRAWL_SDK_MAX_RETRIES", "1"))
    app = FirecrawlApp(api_key=api_key, timeout=request_timeout, max_retries=sdk_retries)

    if "--seed-audit" in sys.argv:
        try:
            preflight_firecrawl_connectivity(app)
            audit_seed_sources(app)
        except FirecrawlAuthenticationError as e:
            print(f"Error: {e}")
            print("Aborting seed audit. FIRECRAWL_API_KEY appears invalid or expired.")
        except FirecrawlConnectivityError as e:
            print(f"Error: {e}")
            print("Aborting seed audit due connectivity failure.")
        return

    if "--discover-seeds" in sys.argv:
        requested_conditions = _requested_discovery_conditions()
        try:
            preflight_firecrawl_connectivity(app)
            audit_seed_sources(
                app,
                include_discovery=True,
                condition_ids=requested_conditions,
            )
        except FirecrawlAuthenticationError as e:
            print(f"Error: {e}")
            print("Aborting seed discovery. FIRECRAWL_API_KEY appears invalid or expired.")
        except FirecrawlConnectivityError as e:
            print(f"Error: {e}")
            print("Aborting seed discovery due connectivity failure.")
        return

    try:
        if "--skip-map" not in sys.argv:
            preflight_firecrawl_connectivity(app)
            map_sources(app)
        else:
            print("Skipping Mapping Phase (--skip-map provided). Using existing database.")
    except FirecrawlAuthenticationError as e:
        print(f"Error: {e}")
        print("Aborting crawl early. FIRECRAWL_API_KEY appears invalid or expired.")
        return
    except FirecrawlConnectivityError as e:
        print(f"Error: {e}")
        print("Aborting crawl early to avoid burning retries in a network-blocked environment.")
        return
        
    chunk_size = int(os.environ.get("CRAWL_CHUNK_SIZE", "100"))
    max_workers = int(os.environ.get("CRAWL_MAX_WORKERS", "10"))
    print(
        f"Runtime config: chunk_size={chunk_size}, workers={max_workers}, "
        f"min_relevance={MIN_RELEVANCE_SCORE}, min_chars={MIN_MARKDOWN_CHARS}"
    )
    scrape_phase(app, chunk_size=chunk_size, max_workers=max_workers)

if __name__ == "__main__":
    main()
