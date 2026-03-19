"""
Skill: LLMPoweredDataStructurer
Source: https://youtube.com/watch?v=ul1hsxTzcqY
Title: AWS re:Invent 2024 - Structured analysis from unstructured data pipelines (AIM277)
Added: 2026-03-19
"""

import requests
import json
import time
import re

# --- Mock Services for demonstration/testing without external APIs ---
class MockWhisperServer:
    """Simulates a self-hosted Whisper API endpoint for transcription."""
    def transcribe(self, audio_data: bytes) -> str:
        # Simplified simulation: ignores actual audio_data, always returns same WebVTT
        # In a real scenario, this would involve an actual transcription model.
        _ = audio_data # audio_data is not used in this mock
        return """WEBVTT

00:00:01.000 --> 00:00:05.000
Hey everybody, it's Denis Coady from Redpanda.

00:00:05.500 --> 00:00:10.000
I want to show you how to leverage Gen AI for unstructured data.

00:00:10.500 --> 00:00:15.000
We often have customer conversations about pain points and feature requests.
"""

class MockBedrockLLM:
    """Simulates an AWS Bedrock LLM invocation for structuring data."""
    def invoke_model(self, prompt: str, model_id: str, max_tokens: int = 4096) -> dict:
        _ = (model_id, max_tokens) # Not used in mock, but matches signature
        # Simulate different responses based on prompt keywords, focusing on the desired JSON structure.
        if "feature requests" in prompt and "pain points" in prompt:
            structured_response = {
                "feature_requests": ["real-time streaming platform", "Kafka compatibility"],
                "pain_points": ["difficulty managing large data volumes", "cost of transcription"],
                "use_cases": ["analytical insights from customer calls"]
            }
        elif "marketing wants to see trends" in prompt:
            structured_response = {
                "marketing_trends": ["real-time analytics", "customer engagement"],
                "popular_features": ["streaming platform"]
            }
        else:
            structured_response = {"extracted_data": "generic response from LLM based on prompt"}
            
        # Bedrock response format for Anthropic Claude typically contains 'completion' field.
        return {"completion": json.dumps(structured_response)}

# --- Main Implementation ---
class LLMPoweredDataStructurer:
    """
    Builds a reusable Python skill for transcribing audio and structuring
    the resulting text using an LLM (e.g., AWS Bedrock) via a meta-prompting approach.
    """
    def __init__(self, whisper_api_url: str = None, bedrock_api_endpoint: str = None, aws_auth_headers: dict = None):
        """
        Initializes the data structurer.
        
        Args:
            whisper_api_url (str, optional): URL for a self-hosted Whisper API (e.g., "http://localhost:8000/transcribe"). 
                                             If None, a mock Whisper service is used for testing.
            bedrock_api_endpoint (str, optional): Base URL for AWS Bedrock's InvokeModel API 
                                                  (e.g., "https://bedrock-runtime.us-east-1.amazonaws.com").
                                                  If None, a mock Bedrock service is used for testing.
            aws_auth_headers (dict, optional): Dictionary of AWS SigV4 authentication headers.
                                               Required for actual Bedrock API calls via requests. 
                                               Example: {'X-Amz-Date': '...', 'Authorization': '...'}.
                                               NOTE: Generating these headers manually for `requests` is complex 
                                               and usually handled by `boto3`. This implementation assumes 
                                               they are pre-generated or used in a controlled environment.
        """
        self.whisper_api_url = whisper_api_url
        self.bedrock_api_endpoint = bedrock_api_endpoint
        self.aws_auth_headers = aws_auth_headers if aws_auth_headers is not None else {}
        
        # Use mock services if API URLs are not provided (for testing/demo)
        self._whisper_service = MockWhisperServer() if not whisper_api_url else None
        self._bedrock_service = MockBedrockLLM() if not bedrock_api_endpoint else None

    def _call_whisper_api(self, audio_data: bytes, content_type: str = "audio/wav") -> str:
        """
        Calls the Whisper transcription API (self-hosted or mock).
        
        Args:
            audio_data (bytes): The raw audio file content.
            content_type (str): The Content-Type header for the audio data.
            
        Returns:
            str: The transcribed text in WebVTT format.
        
        Raises:
            ValueError: If Whisper API URL not provided and mock service is disabled.
            requests.exceptions.RequestException: If the API call fails.
        """
        if self._whisper_service:
            return self._whisper_service.transcribe(audio_data)
        
        if not self.whisper_api_url:
            raise ValueError("Whisper API URL not provided, and mock service is disabled.")

        headers = {"Content-Type": content_type}
        # A self-hosted Whisper API might expect multipart/form-data or raw audio bytes.
        # This assumes raw bytes for simplicity, adjust for specific server implementation.
        response = requests.post(self.whisper_api_url, data=audio_data, headers=headers, timeout=300)
        response.raise_for_status() # Raise an exception for HTTP errors
        return response.text

    def _call_bedrock_api(self, prompt_text: str, model_id: str = "anthropic.claude-v2", max_tokens: int = 4096) -> dict:
        """
        Invokes an LLM model on AWS Bedrock (or mock).
        
        NOTE: This direct `requests` implementation for AWS Bedrock requires manual AWS SigV4
        request signing for authentication, which is complex and usually handled by `boto3`.
        This implementation assumes pre-generated `aws_auth_headers` or a test environment.
        For production, `boto3` is highly recommended for proper authentication and error handling.
        
        Args:
            prompt_text (str): The prompt to send to the LLM.
            model_id (str): The identifier of the Bedrock model to use (e.g., "anthropic.claude-v2").
            max_tokens (int): The maximum number of tokens to generate in the response.
            
        Returns:
            dict: The JSON response from the Bedrock API, containing the 'completion'.
            
        Raises:
            ValueError: If Bedrock API endpoint not provided and mock service is disabled.
            requests.exceptions.RequestException: If the API call fails.
        """
        if self._bedrock_service:
            return self._bedrock_service.invoke_model(prompt_text, model_id, max_tokens)

        if not self.bedrock_api_endpoint:
            raise ValueError("Bedrock API endpoint not provided, and mock service is disabled.")

        # Bedrock API requires specific payload format for different models.
        # This example payload is tailored for Anthropic Claude models.
        payload = {
            "prompt": f"\n\nHuman: {prompt_text}\n\nAssistant:",
            "max_tokens_to_sample": max_tokens,
            "temperature": 0.1, # Keep temperature low for structured/deterministic output
            "top_k": 250,
            "top_p": 1,
            "stop_sequences": ["\n\nHuman:"] # Crucial to prevent the model from continuing the conversation
        }
        
        headers = {
            "Content-Type": "application/json",
            **self.aws_auth_headers # Include pre-generated AWS SigV4 headers here
        }
        
        # Example Bedrock invocation URL structure: 
        # https://bedrock-runtime.<region>.amazonaws.com/model/<model_id>/invoke
        invoke_url = f"{self.bedrock_api_endpoint}/model/{model_id}/invoke"
        
        # Important: Be respectful of rate limits as advised in the transcript.
        # A simple sleep is used here; in production, consider a proper retry/backoff mechanism.
        time.sleep(1) 

        response = requests.post(invoke_url, headers=headers, json=payload, timeout=60)
        response.raise_for_status() # Raise an exception for HTTP errors
        
        response_data = response.json()
        # For Claude, the completion is typically in the 'completion' field. 
        # Other models might have different response structures.
        return {"completion": response_data.get("completion", "")}

    @staticmethod
    def parse_webvtt_transcript(webvtt_content: str) -> str:
        """
        Strips timestamps and metadata from WebVTT content to extract plain text.
        This makes the transcript cleaner and more suitable for LLM input.
        
        Args:
            webvtt_content (str): The raw WebVTT string.
            
        Returns:
            str: The cleaned plain text transcript.
        """
        lines = webvtt_content.splitlines()
        clean_text_parts = []
        in_segment = False
        for line in lines:
            line = line.strip()
            if not line:
                in_segment = False
                continue
            # Regex to match WebVTT timestamp format (e.g., 00:00:01.000 --> 00:00:05.000)
            if re.match(r'^\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}', line):
                in_segment = True
                continue
            # Skip segment numbers and the initial WEBVTT declaration
            if in_segment and not re.match(r'^\d+$', line) and not line.upper() == "WEBVTT":
                clean_text_parts.append(line)
        return " ".join(clean_text_parts).strip()

    def generate_llm_prompt_from_template(self, cleaned_transcript: str, target_objective: str, output_json_example: dict) -> str:
        """
        Generates a robust LLM prompt for structuring data, mimicking the meta-prompting strategy.
        This function constructs the prompt based on the desired output format and objective,
        embodying the idea of having an 'AI write the prompt for you'.
        
        Args:
            cleaned_transcript (str): The plain text transcript to be analyzed.
            target_objective (str): A description of what insights need to be extracted 
                                    (e.g., "identify feature requests, pain points, and use cases").
            output_json_example (dict): A dictionary representing the desired JSON schema and 
                                        example output for few-shot learning.
            
        Returns:
            str: The fully constructed prompt for the LLM.
        """
        # FIX: Changed json.dumps to not use indent=2 to match the compact JSON expected by unit tests.
        json_example_str = json.dumps(output_json_example)

        prompt_template = f"""
You are an expert analytical assistant specialized in extracting specific insights from customer conversation transcripts.
Your task is to analyze the provided transcript and extract information relevant to the following objective: "{target_objective}".
The output MUST be in strict JSON format, adhering to the structure provided in the example below.
Do not include any other text or explanation outside of the JSON object.

Transcript:
---
{cleaned_transcript}
---

Example of desired JSON output format (few-shot example):

{json_example_str}


Please provide the extracted information in the specified JSON format.
"""
        return prompt_template.strip()

    def process_audio_for_analytics(
        self,
        audio_data: bytes,
        target_objective: str,
        output_json_example: dict,
        whisper_content_type: str = "audio/wav",
        bedrock_model_id: str = "anthropic.claude-v2",
        max_llm_tokens: int = 4096
    ) -> dict:
        """
        Orchestrates the end-to-end pipeline: transcribes audio, structures the text
        using an LLM (via a generated prompt), and returns structured data for analytics.
        
        Args:
            audio_data (bytes): The raw audio file content (e.g., WAV).
            target_objective (str): The analytical goal for structuring the data 
                                    (e.g., "extract product feedback").
            output_json_example (dict): A few-shot example of the desired JSON output schema.
            whisper_content_type (str): Content-Type for the audio data sent to Whisper.
            bedrock_model_id (str): The LLM model ID to use on Bedrock (e.g., "anthropic.claude-v2").
            max_llm_tokens (int): Maximum tokens for the LLM's generated response.
            
        Returns:
            dict: The structured data extracted by the LLM, or an error dictionary if JSON parsing fails.
        """
        # 1. Transcribe audio using Whisper
        webvtt_transcript = self._call_whisper_api(audio_data, whisper_content_type)
        cleaned_transcript = self.parse_webvtt_transcript(webvtt_transcript)

        # 2. Generate LLM prompt using a meta-prompting strategy and few-shot examples
        llm_prompt = self.generate_llm_prompt_from_template(cleaned_transcript, target_objective, output_json_example)

        # 3. Structure data using the LLM (AWS Bedrock or mock)
        llm_response = self._call_bedrock_api(llm_prompt, bedrock_model_id, max_tokens=max_llm_tokens)
        
        try:
            # The LLM is instructed to output strict JSON, so we attempt to parse it.
            structured_data = json.loads(llm_response.get("completion", "{}"))
        except json.JSONDecodeError:
            # If the LLM output is not valid JSON, return an error for debugging.
            structured_data = {
                "error": "LLM response was not valid JSON",
                "raw_output": llm_response.get('completion'),
                "prompt_sent": llm_prompt 
            }
            
        return structured_data
