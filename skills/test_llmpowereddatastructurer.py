import sys
from skills.llmpowereddatastructurer import *

import json
import pytest
import requests

# Dummy audio data for testing (minimal WAV header)
# This is not actual audio but enough to satisfy `bytes` type hint.
# In a real scenario, you'd use a small, actual audio file.
dummy_audio_data = b'RIFF\x00\x00\x00\x00WAVEfmt \x10\x00\x00\x00\x01\x00\x01\x00D\xac\x00\x00\x88X\x01\x00\x02\x00\x10\x00data\x00\x00\x00\x00'

def test_parse_webvtt_transcript_basic():
    webvtt_content = """WEBVTT

1
00:00:01.000 --> 00:00:03.000
Hello world.

2
00:00:03.500 --> 00:00:05.000
This is a test.
"""
    expected_text = "Hello world. This is a test."
    assert LLMPoweredDataStructurer.parse_webvtt_transcript(webvtt_content) == expected_text

def test_parse_webvtt_transcript_with_extra_lines_and_metadata():
    webvtt_content = """WEBVTT
Kind: captions
Language: en

NOTE
This is a note for testing.

1
00:00:01.234 --> 00:00:05.678
- Customer: We need better analytics.

2
00:00:06.000 --> 00:00:09.100
- Sales: I agree, insights are key.

"""
    expected_text = "- Customer: We need better analytics. - Sales: I agree, insights are key."
    assert LLMPoweredDataStructurer.parse_webvtt_transcript(webvtt_content) == expected_text

def test_parse_webvtt_transcript_empty():
    webvtt_content = "WEBVTT\n\n"
    assert LLMPoweredDataStructurer.parse_webvtt_transcript(webvtt_content) == ""

def test_generate_llm_prompt_from_template_product_manager():
    structurer = LLMPoweredDataStructurer() # Uses mock services by default
    transcript = "The customer mentioned problems with scaling and requested more features."
    objective = "extract feature requests and pain points"
    output_example = {
        "feature_requests": [""],
        "pain_points": [""],
        "sentiment": ""
    }
    prompt = structurer.generate_llm_prompt_from_template(transcript, objective, output_example)
    
    assert "You are an expert analytical assistant" in prompt
    assert "output MUST be in strict JSON format" in prompt
    # The expected JSON string for the few-shot example should be compact now
    assert f""""feature_requests": [""]""" in prompt
    assert f"Transcript:\n---\n{transcript}\n---" in prompt
    assert f"objective: \"{objective}\"" in prompt

def test_generate_llm_prompt_from_template_marketing():
    structurer = LLMPoweredDataStructurer()
    transcript = "Users love the new UI but want more integrations."
    objective = "identify trends and popular features"
    output_example = {
        "marketing_trends": [""],
        "popular_features": [""],
        "customer_sentiment": ""
    }
    prompt = structurer.generate_llm_prompt_from_template(transcript, objective, output_example)
    
    assert "marketing wants to see trends" not in prompt # Should use the provided objective directly
    assert f"objective: \"{objective}\"" in prompt
    # The expected JSON string for the few-shot example should be compact now
    assert f""""marketing_trends": [""]""" in prompt

def test_process_audio_for_analytics_mocked_services():
    structurer = LLMPoweredDataStructurer() # Uses mock services by default
    
    target_objective = "extract feature requests, pain points, and use cases"
    output_json_example = {
        "feature_requests": ["example feature"],
        "pain_points": ["example pain point"],
        "use_cases": ["example use case"]
    }
    
    result = structurer.process_audio_for_analytics(dummy_audio_data, target_objective, output_json_example)
    
    assert isinstance(result, dict)
    assert "feature_requests" in result
    assert "pain_points" in result
    assert "use_cases" in result
    assert result["feature_requests"] == ["real-time streaming platform", "Kafka compatibility"]
    assert result["pain_points"] == ["difficulty managing large data volumes", "cost of transcription"]
    assert result["use_cases"] == ["analytical insights from customer calls"]

def test_process_audio_for_analytics_invalid_llm_json_output():
    structurer = LLMPoweredDataStructurer()
    # Temporarily override the mock Bedrock service to return invalid JSON
    original_invoke_model = structurer._bedrock_service.invoke_model
    structurer._bedrock_service.invoke_model = lambda p, m, tok: {"completion": "this is not json"}

    target_objective = "extract anything"
    output_json_example = {"key": "value"}

    result = structurer.process_audio_for_analytics(dummy_audio_data, target_objective, output_json_example)
    
    structurer._bedrock_service.invoke_model = original_invoke_model # Restore mock

    assert "error" in result
    assert result["error"] == "LLM response was not valid JSON"
    assert "raw_output" in result
    assert result["raw_output"] == "this is not json"
    assert "prompt_sent" in result

def test_real_api_call_without_url_raises_error():
    # Test _call_whisper_api directly to ensure error handling when no URL and no mock
    structurer_no_mocks = LLMPoweredDataStructurer(whisper_api_url=None, bedrock_api_endpoint=None)
    structurer_no_mocks._whisper_service = None # Disable mock forcefully

    with pytest.raises(ValueError, match="Whisper API URL not provided, and mock service is disabled."):
        structurer_no_mocks._call_whisper_api(dummy_audio_data)

    structurer_no_mocks._bedrock_service = None # Disable mock forcefully
    with pytest.raises(ValueError, match="Bedrock API endpoint not provided, and mock service is disabled."):
        structurer_no_mocks._call_bedrock_api("test prompt")
