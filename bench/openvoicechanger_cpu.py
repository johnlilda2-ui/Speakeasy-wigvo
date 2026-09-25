#!/usr/bin/env python3
"""
Isolated OpenVoiceChanger CPU realtime benchmark.

This script intentionally does NOT touch WIGVO's runtime code. It clones the
public OpenVoiceChanger project into /tmp, loads a public RVC checkpoint, and
measures streaming inference time for several live-audio chunk sizes.

Pass condition:
    p95 processing time <= chunk duration
    and max processing time <= 1.25 * chunk duration

This is a CPU throughput benchmark, not an end-to-end phone/Agora latency test.
"""

from __future__ import annotations

import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen

OVC_REPO = "https://github.com/sioaeko/OpenVoiceChanger.git"
MODEL_URL = (
    "https://huggingface.co/PhoenixStormJr/"
    "Megaman-NT-Warrior-Dr-Wily-RVC/resolve/main/DrWily.pth?download=true"
)
HUBERT_URL = (
    "https://huggingface.co/lj1995/VoiceConversionWebUI/"
    "resolve/main/hubert_base.pt?download=true"
)

ROOT = Path("/tmp/ovc-benchmark")
SOURCE = ROOT / "OpenVoiceChanger"
MODELS = ROOT / "models"
MODEL_PATH = MODELS / "DrWily.pth"
HUBERT_PATH = MODELS / "hubert_base.pt"

SAMPLE_RATE = 48_000
CONTEXT_SECONDS = 0.14


def run(cmd: list[str], cwd: Path | None = None) -> None:
    print("$", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=cwd, check=True)


def download(url: str, target: Path) -> None:
    if target.exists() and target.stat().st_size > 1024:
        print(f"Using cached {target} ({target.stat().st_size / 1024 / 1024:.1f} MiB)", flush=True)
        return

    target.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {target.name} ...", flush=True)
    with urlopen(url, timeout=60) as src, target.open("wb") as dst:
        total = src.headers.get("Content-Length")
        total_i = int(total) if total else None
        done = 0
        last_print = 0.0
        while True:
            block = src.read(1024 * 1024)
            if not block:
                break
            dst.write(block)
            done += len(block)
            now = time.monotonic()
            if now - last_print >= 2.0:
                if total_i:
                    pct = done / total_i * 100
                    print(f"  {done / 1024 / 1024:.1f}/{total_i / 1024 / 1024:.1f} MiB ({pct:.0f}%)", flush=True)
                else:
                    print(f"  {done / 1024 / 1024:.1f} MiB", flush=True)
                last_print = now
    print(f"Saved {target} ({target.stat().st_size / 1024 / 1024:.1f} MiB)", flush=True)


def summarize(times_ms: list[float], chunk_ms: float) -> dict[str, float]:
    ordered = sorted(times_ms)
    p50 = statistics.median(ordered)
    p95_index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * 0.95 + 0.5)))
    p95 = ordered[p95_index]
    maximum = max(ordered)
    return {
        "chunk_ms": chunk_ms,
        "p50_ms": p50,
        "p95_ms": p95,
        "max_ms": maximum,
        "p95_ratio": p95 / chunk_ms,
        "max_ratio": maximum / chunk_ms,
    }


def main() -> int:
    ROOT.mkdir(parents=True, exist_ok=True)
    if not SOURCE.exists():
        run(["git", "clone", "--depth", "1", OVC_REPO, str(SOURCE)])

    download(MODEL_URL, MODEL_PATH)
    download(HUBERT_URL, HUBERT_PATH)

    venv = ROOT / ".venv"
    first_venv_pass = os.environ.get("OVC_BENCHMARK_VENV") != "1"
    if not venv.exists():
        run([sys.executable, "-m", "venv", str(venv)])

    py = venv / "bin" / "python"
    pip = venv / "bin" / "pip"

    if first_venv_pass:
        run([str(py), "-m", "pip", "install", "--upgrade", "pip", "setuptools", "wheel"])
        run([str(pip), "install", "-r", "backend/requirements.txt"], cwd=SOURCE)
        run(
            [
                str(pip),
                "install",
                "--no-deps",
                "git+https://github.com/RVC-Project/Retrieval-based-Voice-Conversion",
            ],
            cwd=SOURCE,
        )

        # The workflow invokes this file with the runner's Python, while the
        # dependencies above are installed into the dedicated venv. Re-enter
        # the benchmark with that venv interpreter so imports and runtime
        # libraries come from the same environment that was just installed.
        env = os.environ.copy()
        env["OVC_BENCHMARK_VENV"] = "1"
        os.execve(str(py), [str(py), str(Path(__file__).resolve()), *sys.argv[1:]], env)

    os.environ.update(
        {
            "OVC_MODELS_DIR": str(MODELS),
            "OVC_HUBERT_PATH": str(HUBERT_PATH),
            "OVC_RMVPE_ROOT": str(MODELS / "rmvpe"),
            "OVC_RVC_STREAM_CONTEXT_SECONDS": str(CONTEXT_SECONDS),
            "OVC_RVC_INDEX_RATE": "0.0",
            "OVC_RVC_FILTER_RADIUS": "3",
            "OVC_RVC_RMS_MIX_RATE": "0.25",
            "OVC_RVC_PROTECT": "0.33",
            "OVC_RVC_ALLOW_UNSAFE_CHECKPOINTS": "false",
        }
    )

    sys.path.insert(0, str(SOURCE))
    import numpy as np
    import torch
    from backend.services.rvc_processor import RvcProcessor

    cpu_count = os.cpu_count() or 2
    thread_count = max(1, min(cpu_count, 4))
    torch.set_num_threads(thread_count)

    print()
    print("=== CPU BENCHMARK ENVIRONMENT ===")
    print(f"CPU count visible: {cpu_count}")
    print(f"PyTorch CPU threads: {thread_count}")
    print(f"Sample rate: {SAMPLE_RATE}")
    print(f"Streaming context: {CONTEXT_SECONDS:.2f}s")
    print()

    processor = RvcProcessor(str(MODEL_PATH))
    print("Loaded model:", processor.describe(), flush=True)

    # Deterministic speech-like test signal: voiced carrier + harmonics + slow
    # amplitude modulation. Throughput is the target metric here.
    duration_s = 1.0
    n = int(SAMPLE_RATE * duration_s)
    t = np.arange(n, dtype=np.float32) / SAMPLE_RATE
    carrier = (
        0.30 * np.sin(2 * np.pi * 145.0 * t)
        + 0.12 * np.sin(2 * np.pi * 290.0 * t)
        + 0.07 * np.sin(2 * np.pi * 435.0 * t)
    )
    envelope = 0.55 + 0.45 * np.sin(2 * np.pi * 3.2 * t) ** 2
    signal = np.clip(carrier * envelope, -0.9, 0.9).astype(np.float32)

    results: list[dict[str, float]] = []
    failed = False

    for chunk_size in (2048, 4096, 8192):
        processor.release_stream("benchmark")
        chunk = signal[:chunk_size]
        chunk_ms = chunk_size / SAMPLE_RATE * 1000.0
        print()
        print(f"=== chunk={chunk_size} ({chunk_ms:.2f} ms) ===")

        # Warmup removes one-time model/runtime initialization from the steady
        # state measurement.
        processor.warm_up(SAMPLE_RATE, chunk_size)

        times_ms: list[float] = []
        for i in range(12):
            start = time.perf_counter()
            out = processor.process(
                chunk,
                sample_rate=SAMPLE_RATE,
                stream_id="benchmark",
                f0_method="pm",
            )
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            if len(out) != len(chunk):
                raise RuntimeError(f"length mismatch: got {len(out)} expected {len(chunk)}")
            times_ms.append(elapsed_ms)
            print(f"  run {i + 1:02d}: {elapsed_ms:.1f} ms", flush=True)

        summary = summarize(times_ms[2:], chunk_ms)
        results.append(summary)
        print(
            "  p50={p50_ms:.1f} ms | p95={p95_ms:.1f} ms | "
            "max={max_ms:.1f} ms | p95/chunk={p95_ratio:.2f}x | "
            "max/chunk={max_ratio:.2f}x".format(**summary)
        )

        # A <= 1.0 p95 ratio is realtime headroom. The 1.25x max check catches
        # pathological stalls that would accumulate queue latency.
        if summary["p95_ratio"] > 1.0 or summary["max_ratio"] > 1.25:
            failed = True

    print()
    print("=== VERDICT ===")
    if failed:
        print("CPU REALTIME: FAIL")
        print("At least one chunk size cannot stay ahead of live audio reliably.")
        return 2

    print("CPU REALTIME: PASS")
    print("The tested CPU runner stayed ahead of live audio at all tested chunk sizes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
