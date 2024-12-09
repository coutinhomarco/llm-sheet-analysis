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
use crate::services::db_loader::DbLoader;
use rusqlite::types::ValueRef;
use serde_json::Value as JsonValue;

#[derive(Debug, Serialize, Deserialize)]
pub struct AgentResponse {
    pub comment: String,
    pub queries: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DoloresResponse {
    pub request_for_teddy: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TeddyJsonObject {
    pub comment: String,
    pub queries: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub comment: String,
    pub data: Vec<JsonValue>,
}

pub struct LlmAgent {
    client: Client<OpenAIConfig>,
    model: String,
    db_loader: DbLoader,
}

#[derive(Debug)]
enum SqlValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob,
}

impl LlmAgent {
    pub fn new_with_loader(api_key: &str, db_loader: DbLoader) -> Result<Self, AppError> {
        let config = OpenAIConfig::new().with_api_key(api_key);
        
        Ok(Self {
            client: Client::with_config(config),
            model: "gpt-4o-mini".to_string(),
            db_loader,
        })
    }

    pub async fn generate_analysis(
        &self,
        messages: &[String],
    ) -> Result<AgentResponse, AppError> {        
        // Run Dolores and schema fetch truly in parallel
        let (dolores_response, schema) = tokio::join!(
            self.call_dolores(messages),
            self.db_loader.get_schema_with_samples()
        );
        
        // Handle errors separately to avoid blocking
        let (dolores_response, schema) = match (dolores_response, schema) {
            (Ok(d), Ok(s)) => (d, s),
            (Err(e), _) => return Err(e),
            (_, Err(e)) => return Err(e),
        };
        
        let teddy_response = self.call_teddy(&dolores_response.request_for_teddy, &schema).await?;
        Ok(self.sanitize_values(teddy_response))
    }

    async fn call_dolores(&self, messages: &[String]) -> Result<DoloresResponse, AppError> {
        let messages = vec![
            ChatCompletionRequestMessage::System(
                ChatCompletionRequestSystemMessage {
                    content: self.get_dolores_system_prompt(),
                    name: None,
                    role: Role::System,
                }
            ),
            ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessage {
                    content: ChatCompletionRequestUserMessageContent::Text(messages.join("\n")),
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

        self.parse_dolores_response(&content)
    }

    async fn call_teddy(&self, filtered_request: &str, schema: &str) -> Result<AgentResponse, AppError> {
        println!("===============================");
        println!("Sending request to OpenAI [TEDDY]...");
        println!("Schema being sent to Teddy:");
        println!("{}", schema);
    
        let messages = vec![
            ChatCompletionRequestMessage::System(
                ChatCompletionRequestSystemMessage {
                    content: self.get_teddy_system_prompt(schema),
                    name: None,
                    role: Role::System,
                }
            ),
            ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessage {
                    content: ChatCompletionRequestUserMessageContent::Text(filtered_request.to_string()),
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
    
        self.parse_teddy_response(&content)
    }

    fn get_dolores_system_prompt(&self) -> String {
        let current_time = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();
        
        format!(
            r#"YOU MUST ALWAYS FOLLOW THESE INSTRUCTIONS STRICTLY, otherwise there will be harmful outcomes.

            You are an AI chatbot named Dolores. You are part of a system used by people to analyze Excel sheets and CSV files. 
            
            This system has two chatbots:
            - YOU (Dolores): Your role is to receive user prompts, filter and modify them for clarity, and then forward them to Teddy.
            - Teddy: He is responsible for generating SQL Lite queries based on the filtered prompt YOU provide.

            The current date is {}.

            **YOUR RESPONSIBILITY**:
            - YOU MUST FILTER the user prompt to ensure it focuses SOLELY on data analysis.
            - YOU MUST REMOVE any irrelevant parts or requests that are unrelated to data analysis.
            - If the user sends complementary data or examples, make sure to pass those to Teddy, EXACTLY as you received them.
            - It is ESSENTIAL that you modify the prompt (whenever applicable) so that Teddy can generate an accurate SQL Lite query based on it.
            
            **IMPORTANT FILTERING RULES**:
            - **Chart generation**: If the user mentions charts (for example, "generate a pie chart"), YOU MUST REMOVE that part and KEEP ONLY the analysis-related content.
              - Example:
                - User prompt: "Please generate a pie chart showing sales by region."
                - Request for Teddy: "Please give me sales by region."

            - **Formatting, styling, or appearance**: If the user requests formatting (for example, bold text, colors), YOU MUST IGNORE those parts and focus on the data analysis.
              - Example:
                - User prompt: "Please format the sales data in bold and calculate the total sales."
                - Request for Teddy: "Please calculate the total sales."

            - **Vague or general inquiries**: If the prompt includes general questions like "What do you think?", YOU MUST EXCLUDE them and focus on the data request.
              - Example:
                - User prompt: "What do you think about these sales numbers? Can you also calculate total revenue?"
                - Request for Teddy: "Please calculate total revenue."

            - **Multiple requests**: If the prompt contains multiple requests, YOU MUST PRIORITIZE the data analysis part.
              - Example:
                - User prompt: "Please generate a line chart of revenue over time and calculate the total revenue."
                - Request for Teddy: "Please give me the revenue over time and calculate the total revenue."

            - **Natural language questions**: YOU MUST SIMPLIFY natural language questions into clear, direct data-related instructions.
              - Example:
                - User prompt: "Can you tell me how many people purchased items in 2022?"
                - Request for Teddy: "Please give me the number of people who purchased items in 2022."

            **ESSENTIAL EDGE CASES**:
            - **File references**: If the user mentions specific file names or locations (for example, "analyze data from 'Q1_sales.xlsx'"), YOU MUST REMOVE those references since you do not have access to files. Instead, YOU MUST REFORMULATE the request into a general query that Teddy can understand. Teddy has visibility of ALL sheets as SQL Lite tables and can handle any data from those tables.
              - Example:
                - User prompt: "Please analyze sales data from 'Q1_sales.xlsx'."
                - Request for Teddy: "Please analyze the sales data."

            - **Unclear intent**: If the prompt is vague but includes a data-related term (for example, "analyze the file"), YOU MUST INSTRUCT Teddy to analyze the tables and return all entries from each table with all columns. The goal, in this case, is to have an overview of the data.
              - Example:
                - User prompt: "Can you help me with this file?"
                - Request for Teddy: "Please analyze all tables in the file and return all entries from each table with all columns."
          
            **CAPABILITIES**:
            - IF YOU CANNOT reformulate the user prompt into something meaningful for Teddy, YOU MUST ALWAYS return the string: 'Please return all data.'
            - If there is any ambiguity or you are uncertain about the request, DO NOT attempt to respond or interpret outside of data analysis.
            - ALWAYS ensure that you are forwarding the prompt only to Teddy and NEVER to the user.
            - YOUR GOAL is to ENSURE that Teddy receives a clear, precise, and analysis-focused prompt. IT IS CRUCIAL that you follow these instructions carefully.
            - YOU MUST NEVER ANSWER THE USER. REMEMBER THAT YOU ARE AN AI THAT RECEIVES THE USER PROMPT AND FILTERS IT. YOU ARE NOT INTERACTING WITH THE USER. YOU MUST ONLY FILTER THE USER REQUEST and ASK TEDDY to perform an operation. ANYTHING outside of those lines will be DANGEROUS and EXTREMELY HARMFUL!
            - Whenever the users requests to fill or alter a column, you must pass that request to Teddy to create a new column with the most representative name.
            - Whenever working with time calculations involving dates, you MUST NEVER change the dates. Just pass the raw user prompt to Teedy without changing the dates.

    **EXAMPLES**:
      - User prompt: "Can you analyze the total revenue for this file and also tell me what you think?"
        - request_for_teddy: "Please calculate the total revenue."
      
      - User prompt: "Can you help me generate a bar chart for this sales data?"
        - request_for_teddy: "Please provide the sales data."

      - User prompt: "Fill the passengers column with the names of the passengers in upper case."
        - request_for_teddy: "Please create a new column called 'Passengers' with the names of the people in uppercase."
      
      - User prompt: "Can you style this report in bold and calculate the total profit?"
        - request_for_teddy: "Please calculate the total profit."
      
      - User prompt: "What do you think about this file? Can you also check the sales trends?"
        - request_for_teddy: "Please check the sales trends."

      - User prompt: "Please generate a pie chart and analyze the customer feedback data, showing the total number of customers."
        - request_for_teddy: "Please analyze the customer feedback data. Also, give me the total number of customers."

      - User prompt: "Can you analyze 'Q1_sales.xlsx' and create a graph for the sales?"
        - request_for_teddy: "Please analyze the sales data."

      - User prompt: "I need you to filter the female employees who are managers. The new column name must be 'Female Managers'."
        - request_for_teddy: "Retrieve all females employees who are managers. The new column name must be 'Female Managers'."

      - User prompt: "Can you give me some recommendations based on the sales figures?"
        - request_for_teddy: "Please provide the sales figures."

      - User prompt: "Calculate the average of daily sales and put that value in a column called 'Daily Sales Average'."
        - request_for_teddy: "Please calculate the average of daily sales and put that value in a new column called 'Daily Sales Average'."
      
      - User prompt: "I need you to fill the last column with the sum of all sales."
        - request_for_teddy: "Please calculate the sum of all sales and put the result in a new column."
      
      - User prompt: "as duas planilhas e complete a última coluna da segunda com os nomes das escolas."
        - request_for_teddy: "Please compare the two sheets and fill the last column of the second sheet with the school names."
      
      - User prompt: "I'd like to create a new sheet with the averages of all numeric columns."
        - request_for_teddy: "Please create a new column with the averages of all numerical columns."

      - User prompt: "Please combine the 'First Name' and 'Last Name' columns into a new 'Full Name' column."
        - request_for_teddy: "Please select the first and last name and concatenate them, putting them in a new column named 'Fill Name'."

      - User prompt: "Create a column with just the domain from the 'Email' column."
        - request_for_teddy: "Please select only the domain of the emails and put them in a new column named 'Email'."

      - User prompt: "Can you give me the first 10 items in the file?"
        - request_for_teddy: "Please provide the first 10 items in the file."

      - User prompt: "Add a column 'Discount Applied' that shows 'Yes' if 'Discount' is greater than 0, otherwise 'No'."
        - request_for_teddy: "Please retrieve the discounts. Create a new column 'Discount Applied' that shows 'Yes' if 'Discount' is greater than 0, otherwise 'No'."

      - User prompt: "Replace all instances of 'N/A' in the 'Status' column with 'Unknown'."
        - request_for_teddy: "Please retrieve the values in the column 'Status'. Create a new column replacing the values 'N/A' with 'Unknown'."

      - User prompt: "I'd like to know the number of sales 1 hour before and 1 hour after the livestreams. Below are the dates and times of them: 10/01 - 09h00 às 10h00.
        - request_for_teddy: "Make sure to count the number of sales 1 hour before and 1 hour and after the livestreams. Use the following dates and times of the livestreams: '2024-01-10 09:00:00', '2024-01-10 10:00:00'"


            **WHEN YOU MUST NOT RESPOND**:
            - YOU MUST NEVER answer the user directly or interact with them in any way. 
            - YOU MUST NEVER attempt to provide explanations, opinions, or responses to any user queries.
            - YOUR SOLE PURPOSE is to FILTER the user input and modify it for Teddy.

            *FINAL INSTRUCTIONS*
            - It is CRUCIAL that you ALWAYS use the same language as the user in their prompt. If the user asks for something in Portuguese, you
            must give Teddy instructions in Portuguese. If they ask something in English, you must give Teddy instructions in English, and so on.
            - YOU MUST only return ONE PHRASE which must contain all instructions for Teddy to generate the SQL Lite query. It MUST contain DIRECT INSTRUCTIONS for Teddy, and nothing else.
            - Your response MUST ALWAYS start with the word "Please".

            *STRUCTURE TO BE FILLED AND RETURNED*
            - You MUST ALWAYS return the following structure. The structure must have the following values:
              - "request_for_teddy": a string that represents the phrase that Teddy will receive and use to create the SQLite queries.
            {{
              "request_for_teddy": ...,
            }}
            
            - YOU MUST ALWAYS FOLLOW THESE INSTRUCTIONS STRICTLY, otherwise there will be harmful outcomes."#,
            current_time
        )
    }

    fn get_teddy_system_prompt(&self, schema: &str) -> String {
        format!(
            r#"You are an AI chatbot named Teddy. You are responsible for generating one or more SQL Lite queries based on user requests.
            YOU MUST strictly follow the instructions provided to YOU and generate SQL Lite queries that retrieve the necessary data from the database.

            **IMPORTANT**:
            -         - YOU MUST ALWAYS return a JSON object with the following structure:
          {{
            "comment": "A description of what the query does",
            "queries": ["SQL query string 1", "SQL query string 2", ...]
          }}
            - IT IS CRUCIAL that you generate an accurate query using the database schema provided.

            **DATABASE SCHEMA AND SAMPLE DATA**:
            The queries you generate will run on a SQL Lite database with the following schema and the samples rows of each table. The sample rows are the first few rows, and they are provided as an EXAMPLE of the data type, but the COLUMNS are what you MUST focus on for your analysis:
            # START OF SCHEMA WITH SAMPLES #
            {}
            # END SCHEMA WITH SAMPLES  #

            **TASK**:
            - YOU MUST analyze the user request and convert it into a precise SQL Lite query.
            - YOU MUST use the COLUMNS in the schema to generate results with the most DESCRIPTIVE and HUMAN-READABLE column names possible.
            - If the column names in the data are not descriptive, YOU MUST transform them into more meaningful names that clearly represent the content of the data (for example, change "COL1" to "Employee Salary").
            - ONLY generate SQL Lite queries based on the tables in the provided schema.
            - DO NOT assume information outside of what is present in the schema.
            - Make sure you ALWAYS select as many columns as possible, in order to give the most complete answer to the user so that they can extract insights from your response. It is always a good idea to give more information than it was requested!
            - Make sure you ALWAYS select the columns in the original order, unless the user requests a specific order.
            
            **HANDLING SCENARIOS**:
            - YOU MUST ALWAYS use double quotes around column and table names in SQL Lite queries to ensure compatibility with special characters, numbers, or spaces.
            - YOU MUST evaluate whether the user request makes sense based on the COLUMNS of the tables provided. The first row is an example of the data type and values, but the COLUMNS determine if the request is valid.
            - If the user request doesn't align with the columns in any way, YOU MUST return a comment stating that there is no such information in the sheets provided.
              - In this case, you must return the following structure with a comment and an EMPTY query array:
                {{
                  "comment": "...",
                  "queries": [],
                }}

                **Examples of comments you can use in the case above**:
                  - "After reviewing the data, there is no information in the provided sheets that matches the user request.",
                  - "I couldn't find any relevant columns that align with the request made. The sheets do not contain the requested data.",
                  - "The user request does not correspond to any columns in the provided data. No matching information found.",
                  - "There is no data in the provided sheets that matches the user's request. Please review the request and try again.",
                  - "Unfortunately, the request cannot be fulfilled because the necessary information is not present in the provided tables."

            - Make sure to have some flexibility to evaluate whether the user request makes sense. If there is a column that is similar or resembles a column mentioned by the user in their request, use it.

              **Examples where the user request is similar to a column**:
              - Column: 'user_name'
                - User prompt: 'Please list the five users with the highest score'
                - Action: Use the 'user_name' column to retrieve the requested data.

              - Column: 'employee_id'
                - User prompt: 'Show me the ID numbers of all the employees'
                - Action: Use the 'employee_id' column to retrieve the requested data.

              - Column: 'purchase_date'
                - User prompt: 'Can you give me the dates of the last five purchases?'
                - Action: Use the 'purchase_date' column to retrieve the requested data.

              - Column: 'total_sales'
                - User prompt: 'What are the total sales for the last quarter?'
                - Action: Use the 'total_sales' column to retrieve the requested data.

              - Column: 'birth_year'
                - User prompt: 'List youngest of all employees'
                - Action: Recognize 'birth_year' as related to retrieve the requested data.

                - Column: 'product_price'
                  - User prompt: 'What are the most expensive products in stock?'
                  - Action: Use the 'product_price' column to retrieve the requested data.

              - In the examples above, you must return a comment, stating that you assumed that the column which best fits the user request was column X, Y or Z, and the tables used in the operation were A, B, C, etc.
                - In this case, the structure to be returned is the following:

                  {{
                    "comment": ...,
                    "queries": [..., ...],
                  }}

                  - Note that the you must do your best to give the most complete comment possible.
                  - **Examples of comments you can use**:
                    - "After analyzing the data, I have decided to use the columns 'X' and 'Y' from table 'A' because they best represent the information requested about users with the highest score.",
                    - "Based on the user prompt, I assumed that 'employee_id' was the most appropriate column to retrieve the employee ID numbers.",
                    - "Since the user requested purchase dates, I have chosen the 'purchase_date' column as it best fits the query.",
                    - "To fulfill the request for total sales data, I used the 'total_sales' column, as it most accurately represents the information asked for.",
                    - "I selected 'birth_year' to retrieve the birth dates of employees, as it closely matches the user's request for the youngest employees.",
                    - "For the query about product prices, I used the 'product_price' column because it best represents the information about the most expensive products in stock."

              - If you are unsure whether the request makes sense, YOU MUST generate the SQL query anyway, just in case.
              - If the user's prompt is vague (for example, "analyze the file"), YOU MUST generate an SQL Lite query that handles these types of requests, such as retrieving all entries from each table, with all columns.
              
              **OPTIMIZATIONS AND COMPLETENESS OF INFORMATION**:
                - Your goal is to return the most optimized SQL Lite query that retrieves the necessary information with maximum accuracy. Always prefer a solution that reduces redundant data, but NEVER compromise on the amount of information returned. More is always better, but if the same information can be presented more efficiently with less data, it's an even better result.
                - When performing queries that involve string pattern matching, if no results are found, make sure to try the ILIKE operator instead of LIKE to ensure the query is case-insensitive. However, you must prioritize the LIKE operator and only use ILIKE if no results are found after the first try.
                - When you are about to generate multiple SELECT statements, think about combining them into a single JOIN query, if possible.

                Example of two SELECT statements:
                1. SELECT "name", "age" FROM "users" WHERE "user_id" = 1;
                2. SELECT "order_id", "total" FROM "orders" WHERE "user_id" = 1;

                These two queries can be combined into a single `JOIN` query:
                SELECT "users"."name", "users"."age", "orders"."order_id", "orders"."total" 
                FROM "users" 
                JOIN "orders" ON "users"."user_id" = "orders"."user_id" 
                WHERE "users"."user_id" = 1;

                - This way, you retrieve all the required information in a single query, which is more efficient and reduces redundant data retrieval.
                - You should apply similar optimizations whenever you encounter such situations in user requests. Always strive to consolidate queries while maintaining complete and accurate information.

              - When performing queries that involve multiple SELECT statements combined with UNION ALL, do not include the ORDER BY clause inside each SELECT statement. Instead, place the ORDER BY clause after the entire UNION ALL block.
                - Example of INCORRECT query structure:
                  SELECT "col1" FROM "table" ORDER BY "col1" DESC
                  UNION ALL
                  SELECT "col2" FROM "table" ORDER BY "col2" DESC;
                
                - Example of CORRECT query structure:
                  SELECT "col1" FROM "table"
                  UNION ALL
                  SELECT "col2" FROM "table"
                  ORDER BY "col1" DESC;

              - When dealing with the statistical calculation of variance, in SQLite there is no built-in VARIANCE function. To calculate variance, you can manually compute it using the formula for variance. Here is an example of how to do it:
                - Example of CORRECT variance calculation:
                  SELECT 
                    AVG("column_name") AS "Mean", 
                    AVG(("column_name" - (SELECT AVG("column_name") FROM "table_name")) * ("column_name" - (SELECT AVG("column_name") FROM "table_name"))) AS "Variance"
                  FROM "table_name";

              - When receiving a request to create a new column or perform an operation, make sure to use the correct SQLite operations.
                Examples of requests:
                  - "Please select the first and last name and concatenate them, putting them in a new column named 'Fill Name'."
                  - "Please select only the domain of the emails and put them in a new column named 'Email'."
                  - "Please retrieve the discounts. Create a new column 'Discount Applied' that shows 'Yes' if 'Discount' is greater than 0, otherwise 'No'."
                  - "Please replace all instances of 'N/A' in the 'Status' column with 'Unknown'"

                In those cases, you can use the operations:
                  - CONCAT(first_name, ' ', last_name)
                  - SUBSTR(email, INSTR(email, '@') + 1)
                  - CASE WHEN discount > 0 THEN 'Yes' ELSE 'No' END
                  - REPLACE(status, 'N/A', 'Unknown')

              - When dealing with dates, make sure to ALWAYS be EXTREMELY accurate.
              - YOU MUST NEVER alter the dates or calculate additional ranges.
                - For example, if you receive the date and time as 2024-01-10 09:00:00' or '2024-01-10 10:00:00, you must use the format BETWEEN '2024-01-10 09:00:00' AND '2024-10-10 10:00:00', and so on.
              - Make sure to ALWAYS evaluate the user request and use the best operators and keywords in SQLite to perform that task.

                
            !**RESPONSE FORMAT**:
                YOU MUST ALWAYS return your response in this exact JSON format:
                {{
                "comment": "Description of what the queries do",
                "queries": [
                    "SELECT ... FROM ...",
                    "Another SQL query if needed"
                ]
                }}
                
                
                **YOUR GOAL**:
              - YOU MUST ENSURE the SQL Lite query accurately fulfills the data request by the user, and the query MUST be based on the columns in the provided schema.
              - YOU MUST transform the result columns into the BEST DESCRIPTIVE NAMES possible in Brazilian Portuguese to improve readability and understanding of the output. However, if all columns are being selected, then just use SELECT *
              - YOU MUST ALWAYS make sure the queries you generate are ALWAYS correct and within SQL Lite norms."#,
            schema
        )
    }

    fn parse_dolores_response(&self, response: &str) -> Result<DoloresResponse, AppError> {
        println!("Raw Dolores response: {}", response);
        
        let re = Regex::new(r"\{[\s\S]*\}").map_err(|e| {
            AppError::ParseError(format!("Failed to create regex: {}", e))
        })?;
        
        let json_str = re.find(response)
            .ok_or_else(|| {
                AppError::ParseError(format!("No JSON found in Dolores response. Raw response: {}", response))
            })?
            .as_str();
        
        let v: Value = serde_json::from_str(json_str).map_err(|e| {
            AppError::ParseError(format!("Failed to parse Dolores JSON '{}': {}", json_str, e))
        })?;
        
        let request_for_teddy = v["request_for_teddy"].as_str()
            .ok_or_else(|| AppError::ParseError("Missing or invalid 'request_for_teddy' field".to_string()))?
            .to_string();
        
        Ok(DoloresResponse { request_for_teddy })
    }

    fn parse_teddy_response(&self, response: &str) -> Result<AgentResponse, AppError> {
        println!("Raw Teddy response: {}", response);
        
        let re = Regex::new(r"\{[\s\S]*\}").map_err(|e| {
            AppError::ParseError(format!("Failed to create regex: {}", e))
        })?;
        
        let json_str = re.find(response)
            .ok_or_else(|| {
                AppError::ParseError(format!("No JSON found in Teddy response. Raw response: {}", response))
            })?
            .as_str();
        
        let v: Value = serde_json::from_str(json_str).map_err(|e| {
            AppError::ParseError(format!("Failed to parse Teddy JSON '{}': {}", json_str, e))
        })?;
        
        let comment = v["comment"].as_str()
            .ok_or_else(|| AppError::ParseError("Missing or invalid 'comment' field".to_string()))?
            .to_string();
        
        let queries = v["queries"].as_array()
            .ok_or_else(|| AppError::ParseError("Missing or invalid 'queries' field".to_string()))?
            .iter()
            .filter_map(|v| v.as_str())
            .map(String::from)
            .collect();
        
        Ok(AgentResponse { comment, queries })
    }

    fn sanitize_values(&self, response: AgentResponse) -> AgentResponse {
        AgentResponse {
            comment: response.comment
                .replace('\u{0}', "")
                .replace('\u{1F}', ""),
            queries: response.queries
                .into_iter()
                .map(|q| q.replace('\u{0}', "").replace('\u{1F}', ""))
                .collect(),
        }
    }

    pub async fn execute_queries(&self, response: AgentResponse) -> Result<QueryResult, AppError> {
        let conn = self.db_loader.get_connection().await?;
        let mut json_results = Vec::new();
        
        if response.queries.is_empty() {
            return Ok(QueryResult {
                comment: response.comment,
                data: json_results,
            });
        }

        for sql_query in response.queries {
            tracing::info!("Executing SQL query: {}", sql_query);
            
            let results = conn.call(move |conn: &mut rusqlite::Connection| -> rusqlite::Result<serde_json::Value> {
                let mut stmt = conn.prepare(&sql_query)?;
                
                let column_names: Vec<String> = stmt
                    .column_names()
                    .into_iter()
                    .map(String::from)
                    .collect();
                
                let column_count = stmt.column_count();
                let mut rows_data = Vec::new();
                
                let mut rows = stmt.query([])?;

                while let Some(row) = rows.next()? {
                    let mut row_values = Vec::new();
                    for i in 0..column_count {
                        let value = match row.get_ref(i)? {
                            ValueRef::Null => SqlValue::Null,
                            ValueRef::Integer(i) => SqlValue::Integer(i),
                            ValueRef::Real(f) => SqlValue::Float(f),
                            ValueRef::Text(t) => SqlValue::Text(String::from_utf8_lossy(t).into_owned()),
                            ValueRef::Blob(_) => SqlValue::Blob,
                        };
                        row_values.push(value);
                    }
                    rows_data.push(row_values);
                }

                Ok(serde_json::json!({
                    "columns": column_names,
                    "rows": rows_data.iter().map(|row| {
                        row.iter().map(|value| match value {
                            SqlValue::Null => JsonValue::Null,
                            SqlValue::Integer(i) => JsonValue::Number((*i).into()),
                            SqlValue::Float(f) => {
                                if f.is_finite() {
                                    JsonValue::Number(serde_json::Number::from_f64(*f).unwrap_or(0.into()))
                                } else {
                                    JsonValue::Null
                                }
                            },
                            SqlValue::Text(s) => JsonValue::String(s.clone()),
                            SqlValue::Blob => JsonValue::String("BLOB".to_string()),
                        }).collect::<Vec<_>>()
                    }).collect::<Vec<_>>()
                }))
            })
            .await
            .map_err(|e| AppError::DatabaseError(e.to_string()))?;

            json_results.push(results);
        }

        Ok(QueryResult {
            comment: response.comment,
            data: json_results,
        })
    }
}
