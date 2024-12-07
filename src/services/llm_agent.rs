use regex::Regex;
use serde_json::{self, Value};
use chrono::Utc;

use async_openai::{
    config::OpenAIConfig,
    types::{
        ChatCompletionRequestMessage, Role, CreateChatCompletionRequest,
        ChatCompletionRequestSystemMessage, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent,
    },
    Client,
};
use serde::{Deserialize, Serialize};
use crate::error::AppError;

#[derive(Debug, Serialize, Deserialize)]
pub struct AgentResponse {
    pub comment: String,
    pub queries: Vec<String>,
}

pub struct LlmAgent {
    client: Client<OpenAIConfig>,
    model: String,
}

impl LlmAgent {
    pub fn new(api_key: &str) -> Self {
        let config = OpenAIConfig::new().with_api_key(api_key);
        Self {
            client: Client::with_config(config),
            model: "gpt-4-turbo-preview".to_string(),
        }
    }

    pub async fn generate_analysis(
        &self,
        schema: &str,
        prompt: &str,
    ) -> Result<AgentResponse, AppError> {
        let messages = vec![
            ChatCompletionRequestMessage::System(
                ChatCompletionRequestSystemMessage {
                    content: self.get_system_prompt(schema),
                    name: None,
                    role: Role::System,
                }
            ),
            ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessage {
                    content: ChatCompletionRequestUserMessageContent::Text(prompt.to_string()),
                    name: None,
                    role: Role::User,
                }
            ),
        ];

        let request = CreateChatCompletionRequest {
            model: self.model.clone(),
            messages,
            temperature: Some(0.1),
            ..Default::default()
        };

        let response = self.client
            .chat()
            .create(request)
            .await
            .map_err(|e| AppError::LlmError(e.to_string()))?;

        let content = response.choices[0]
            .message
            .content
            .clone()
            .unwrap_or_default();

        self.parse_response(&content)
    }

    fn get_system_prompt(&self, schema: &str) -> String {
        let current_time = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
        
        format!(r#"
        YOU MUST ALWAYS FOLLOW THESE INSTRUCTIONS STRICTLY, otherwise there will be harmful outcomes.
    
        You are an AI chatbot named Teddy. You are responsible for generating SQL Lite queries based on user requests.
        The current date is {}.
    
        **DATABASE SCHEMA AND SAMPLE DATA**:
        The queries you generate will run on a SQL Lite database with the following schema and sample rows:
        # START OF SCHEMA WITH SAMPLES #
        {}
        # END SCHEMA WITH SAMPLES #
    
        **YOUR TASK**:
        - Generate precise SQL Lite queries based on the user request
        - Use descriptive column names
        - Always select as many relevant columns as possible
        - Return queries in a JSON format with optional comments
    
        **RESPONSE FORMAT**:
        {{
            "comment": "Optional explanation of assumptions or decisions made",
            "queries": ["SQL query 1", "SQL query 2", ...]
        }}
        "#, current_time, schema)
    }

    fn parse_response(&self, response: &str) -> Result<AgentResponse, AppError> {
        // Find JSON object in response using regex
        let re = Regex::new(r"\{[\s\S]*\}").map_err(|e| {
            AppError::ParseError(format!("Failed to create regex: {}", e))
        })?;
        
        let json_str = re.find(response)
            .ok_or_else(|| AppError::ParseError("No JSON found in response".to_string()))?
            .as_str();
        
        // Parse JSON
        let v: Value = serde_json::from_str(json_str).map_err(|e| {
            AppError::ParseError(format!("Failed to parse JSON: {}", e))
        })?;
        
        // Extract fields
        let comment = v["comment"].as_str()
            .unwrap_or("No comment provided")
            .to_string();
        
        let queries = v["queries"].as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        
        Ok(AgentResponse { comment, queries })
    }
}
