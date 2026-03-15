"""Hugging Face model download count collectors.

Tracks monthly download counts for key AI models as a proxy for
ML/AI adoption and model popularity trends.

No API key required.  Docs: https://huggingface.co/docs/hub/api
"""
from __future__ import annotations

import threading
import time

import pandas as pd
import requests

from signal_noise.collector.base import BaseCollector, CollectorMeta

_lock = threading.Lock()
_last_request: float = 0.0
_MIN_INTERVAL: float = 2.0


def _throttled_get(url: str, timeout: int = 30) -> dict:
    global _last_request
    with _lock:
        wait = _MIN_INTERVAL - (time.monotonic() - _last_request)
        if wait > 0:
            time.sleep(wait)
        resp = requests.get(url, timeout=timeout)
        _last_request = time.monotonic()
    resp.raise_for_status()
    return resp.json()


# (model_id, collector_name, display_name)
_MODELS: list[tuple[str, str, str]] = [
    ("sentence-transformers/all-MiniLM-L6-v2", "hf_minilm", "HF Downloads: all-MiniLM-L6-v2"),
    ("google-bert/bert-base-uncased", "hf_bert", "HF Downloads: bert-base-uncased"),
    ("openai/whisper-large-v3", "hf_whisper", "HF Downloads: whisper-large-v3"),
    ("meta-llama/Llama-3.3-70B-Instruct", "hf_llama3_70b", "HF Downloads: Llama-3.3-70B"),
    ("mistralai/Mistral-7B-Instruct-v0.3", "hf_mistral_7b", "HF Downloads: Mistral-7B"),
    ("stabilityai/stable-diffusion-xl-base-1.0", "hf_sdxl", "HF Downloads: SDXL"),
    ("openai/clip-vit-large-patch14", "hf_clip", "HF Downloads: CLIP"),
    ("facebook/bart-large-mnli", "hf_bart_mnli", "HF Downloads: BART-MNLI"),
    ("Qwen/Qwen2.5-72B-Instruct", "hf_qwen_72b", "HF Downloads: Qwen2.5-72B"),
    ("google/gemma-2-9b-it", "hf_gemma_9b", "HF Downloads: Gemma-2-9B"),
    ("black-forest-labs/FLUX.1-dev", "hf_flux", "HF Downloads: FLUX.1-dev"),
    ("deepseek-ai/DeepSeek-R1", "hf_deepseek_r1", "HF Downloads: DeepSeek-R1"),
    ("mistralai/Mistral-7B-v0.1", "hf_mistral_7b_base", "HF Downloads: Mistral-7B-v0.1"),
    ("google/gemma-7b", "hf_gemma_7b", "HF Downloads: Gemma-7B"),
    ("microsoft/phi-2", "hf_phi2", "HF Downloads: phi-2"),
    ("TheBloke/Llama-2-13B-chat-GPTQ", "hf_thebloke_llama2_13b", "HF Downloads: TheBloke Llama-2-13B-GPTQ"),
    ("TheBloke/Mistral-7B-Instruct-v0.2-GPTQ", "hf_thebloke_mistral_7b", "HF Downloads: TheBloke Mistral-7B-GPTQ"),
]


def _make_hf_collector(
    model_id: str, name: str, display_name: str,
) -> type[BaseCollector]:
    class _Collector(BaseCollector):
        meta = CollectorMeta(
            name=name,
            display_name=display_name,
            update_frequency="daily",
            api_docs_url=f"https://huggingface.co/{model_id}",
            domain="technology",
            category="ai",
        )

        def fetch(self) -> pd.DataFrame:
            url = f"https://huggingface.co/api/models/{model_id}"
            data = _throttled_get(url, timeout=self.config.request_timeout)
            downloads = data.get("downloads")
            if downloads is None:
                raise RuntimeError(f"No download count for {model_id}")
            now = pd.Timestamp.now(tz="UTC").floor("D")
            return pd.DataFrame([{"date": now, "value": float(downloads)}])

    _Collector.__name__ = f"HF_{name}"
    _Collector.__qualname__ = f"HF_{name}"
    return _Collector


def get_huggingface_collectors() -> dict[str, type[BaseCollector]]:
    return {
        name: _make_hf_collector(model_id, name, display)
        for model_id, name, display in _MODELS
    }
