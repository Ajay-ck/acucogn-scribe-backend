
import logging
from typing import List, Tuple, Dict, Optional
from pipeline.audio_utils import ensure_wav, transcribe_with_deepgram

from pipeline.gemini_llm import generate_soap, correct_diarization


class MedicalAudioProcessor:
    def __init__(self, audio_dir: str = "recordings")-> None:
        self.audio_dir = audio_dir

    def ensure_wav(self, audio_path: str) -> str:
        return ensure_wav(audio_path)

    def transcribe_file(self, audio_path: str, beam_size: int = 5):
    
        return transcribe_with_deepgram(audio_path, diarize=True)
    def generate_soap(self, transcript: str) -> str:
        return generate_soap(transcript)
    
    def correct_diarization(self, transcript: str) -> str:
        return correct_diarization(transcript)