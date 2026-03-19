# LLMPoweredDataStructurer

**Source:** [AWS re:Invent 2024 - Structured analysis from unstructured data pipelines (AIM277)](https://youtube.com/watch?v=ul1hsxTzcqY)
**Added:** 2026-03-19

Here's the `README.md` documentation for the `LLMPoweredDataStructurer` skill:

---

# LLMPoweredDataStructurer

## What this is
This is a Python skill designed for data engineers to transform unstructured audio data into structured, actionable insights using a two-stage pipeline: **audio transcription** followed by **Large Language Model (LLM) based data structuring**. It leverages a "meta-prompting" approach, where the system dynamically generates optimized LLM prompts based on a specified objective and desired output schema.

## The problem it solves
Data engineers often face the challenge of extracting valuable, quantifiable information from vast amounts of unstructured data sources like customer call recordings, meeting transcripts, or spoken feedback. Manually sifting through these or building rigid rule-based systems for extraction is time-consuming, error-prone, and not scalable.

This skill automates the process of:
1.  Transcribing audio into text.
2.  Intelligently extracting specific entities, themes, or sentiments (e.g., pain points, feature requests, marketing trends, use cases) into a well-defined JSON format, making the data easily queryable and analyzable in downstream systems.

It bridges the gap between raw, unstructured audio and structured data suitable for analytics, reporting, and machine learning applications.

## How to use it
The `LLMPoweredDataStructurer` class orchestrates the transcription and LLM-based structuring process. For demonstration and testing, it can use mock services internally if actual API endpoints are not provided.

### Installation
No external libraries are strictly required beyond `requests` and standard Python modules, but for a real-world scenario with AWS Bedrock, you'd typically use `boto3`.

### Short Code Example

```python
import requests
import json
import time
import re

# --- Mock Services (Included in the original code, for self-contained example) ---
class MockWhisperServer:
    def transcribe(self, audio_data: bytes) -> str:
        _ = audio_data 
        return """WEBVTT

00:00:01.000 --> 00:00:05.000
Hey everybody, it's Denis Coady from Redpanda.

00:00:05.500 --> 00:00:10.000
I want to show you how to leverage Gen AI for unstructured data.

00:00:10.500 --> 00:00:15.000
We often have customer conversations about pain points and feature requests.
"""

class MockBedrockLLM:
    def invoke_model(self, prompt: str, model_id: str, max_tokens: int = 4096) -> dict:
        _ = (model_id, max_tokens)
        if "feature requests" in prompt and "pain points" in prompt:
            structured_response = {
                "feature_requests": ["real-time streaming platform", "Kafka compatibility"],
                "pain_points": ["difficulty managing large data volumes", "cost of transcription"],
                "use_cases": ["analytical insights from customer calls"]
            }
        else:
            structured_response = {"extracted_data": "generic response from LLM based on prompt"}
        return {"completion": json.dumps(structured_response)}

# --- Main Implementation (LLMPoweredDataStructurer class as provided) ---
# (Paste the LLMPoweredDataStructurer class definition here, including all methods)
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
        response = requests.post(self.whisper_api_url, data=audio_data, headers=headers, timeout=300)
        response.raise_for_status() 
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

        payload = {
            "prompt": f"\n\nHuman: {prompt_text}\n\nAssistant:",
            "max_tokens_to_sample": max_tokens,
            "temperature": 0.1, 
            "top_k": 250,
            "top_p": 1,
            "stop_sequences": ["\n\nHuman:"]
        }
        
        headers = {
            "Content-Type": "application/json",
            **self.aws_auth_headers
        }
        
        invoke_url = f"{self.bedrock_api_endpoint}/model/{model_id}/invoke"
        
        time.sleep(1) 

        response = requests.post(invoke_url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        
        response_data = response.json()
        return {"completion": response_data.get("completion", "")}

    @staticmethod
    def parse_webvtt_transcript(webvtt_content: str) -> str:
        """
        Strips timestamps and metadata from WebVTT content to extract plain text.
        """
        lines = webvtt_content.splitlines()
        clean_text_parts = []
        in_segment = False
        for line in lines:
            line = line.strip()
            if not line:
                in_segment = False
                continue
            if re.match(r'^\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}', line):
                in_segment = True
                continue
            if in_segment and not re.match(r'^\d+$', line) and not line.upper() == "WEBVTT":
                clean_text_parts.append(line)
        return " ".join(clean_text_parts).strip()

    def generate_llm_prompt_from_template(self, cleaned_transcript: str, target_objective: str, output_json_example: dict) -> str:
        """
        Generates a robust LLM prompt for structuring data, mimicking the meta-prompting strategy.
        """
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
        """
        webvtt_transcript = self._call_whisper_api(audio_data, whisper_content_type)
        cleaned_transcript = self.parse_webvtt_transcript(webvtt_transcript)

        llm_prompt = self.generate_llm_prompt_from_template(cleaned_transcript, target_objective, output_json_example)

        llm_response = self._call_bedrock_api(llm_prompt, bedrock_model_id, max_tokens=max_llm_tokens)
        
        try:
            structured_data = json.loads(llm_response.get("completion", "{}"))
        except json.JSONDecodeError:
            structured_data = {
                "error": "LLM response was not valid JSON",
                "raw_output": llm_response.get('completion'),
                "prompt_sent": llm_prompt 
            }
            
        return structured_data

# --- Example Usage ---
if __name__ == "__main__":
    # Initialize the structurer (will use mock services if API URLs are None)
    # For real use, provide whisper_api_url, bedrock_api_endpoint, and aws_auth_headers
    structurer = LLMPoweredDataStructurer() 

    # Mock audio data (in a real scenario, this would be actual audio bytes)
    mock_audio_data = b"This is a byte stream simulating audio content."

    # Define the analytical objective
    target_objective = "identify feature requests, pain points, and use cases mentioned by customers"

    # Provide a few-shot example of the desired JSON output structure
    output_schema_example = {
        "feature_requests": ["example_feature_1", "example_feature_2"],
        "pain_points": ["example_pain_point_1"],
        "use_cases": ["example_use_case_1"]
    }

    print("Processing audio for analytics...")
    structured_result = structurer.process_audio_for_analytics(
        audio_data=mock_audio_data,
        target_objective=target_objective,
        output_json_example=output_schema_example
    )

    print("\n--- Structured Data Output ---")
    print(json.dumps(structured_result, indent=2))

    # Example with a different objective for the mock LLM
    print("\nProcessing with a different objective...")
    marketing_objective = "extract key marketing trends and popular features discussed"
    marketing_schema = {
        "marketing_trends": ["trend_1"],
        "popular_features": ["feature_A"]
    }
    marketing_result = structurer.process_audio_for_analytics(
        audio_data=mock_audio_data,
        target_objective=marketing_objective,
        output_json_example=marketing_schema
    )
    print("\n--- Marketing Structured Data Output ---")
    print(json.dumps(marketing_result, indent=2))
```

## What real-world tool this relates to
This skill directly relates to and integrates with:
*   **Transcription Services:** AWS Transcribe, Google Cloud Speech-to-Text, Azure Speech, or self-hosted OpenAI Whisper models.
*   **Large Language Models (LLMs):** Specifically generative LLMs like those available via AWS Bedrock (e.g., Anthropic Claude, Amazon Titan), OpenAI's GPT models, or Google's PaLM/Gemini.
*   **Data Pipelines & Streaming Platforms:** Systems like Redpanda or Apache Kafka, which can ingest audio streams, pass them to this structurer, and then forward the resulting structured JSON to data lakes (e.g., S3), data warehouses (e.g., Snowflake, Redshift), or analytical databases for real-time insights and dashboarding.

It's a foundational component for building intelligent data processing pipelines that derive structured value from vast quantities of previously inaccessible unstructured data.

## Limitations
*   **Authentication & Robustness:** The current direct `requests` implementation for AWS Bedrock assumes pre-generated AWS SigV4 headers. For production, the `boto3` library is highly recommended as it handles AWS authentication, retries, and error patterns automatically and more securely.
*   **LLM Accuracy & Hallucination:** While powerful, LLMs can still produce inaccurate or "hallucinated" information, especially with ambiguous prompts or very long/complex transcripts. Careful prompt engineering, few-shot examples, and potentially post-processing validation are crucial.
*   **Cost & Latency:** API calls to transcription services and LLMs incur costs per use and introduce latency. For high-volume, real-time scenarios, these factors must be carefully considered and optimized (e.g., batching, asynchronous processing, efficient model selection).
*   **Transcript Length:** LLMs have context window limits. Very long audio recordings might result in transcripts that exceed these limits, requiring chunking strategies and careful aggregation of results.
*   **Specific API Implementations:** The `_call_whisper_api` method assumes a certain self-hosted Whisper API interface (raw audio bytes). Real-world Whisper server implementations might expect `multipart/form-data` or other specific formats, which would require adjustments.
*   **Error Handling:** The current error handling for `json.JSONDecodeError` provides basic debugging information. A production-grade system would require more robust error handling, logging, and retry mechanisms for API calls.