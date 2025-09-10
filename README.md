# Sheet Services

A robust Rust-based microservice for analyzing and processing Excel sheets with advanced data manipulation capabilities and AI-powered insights.

## Features

- Excel file processing and analysis
- In-memory SQLite database for efficient data handling
- AI-powered data analysis using OpenAI's GPT models
- RESTful API for file upload and analysis
- Concurrent processing with tokio runtime
- Caching mechanism for improved performance
- Comprehensive error handling and logging

## Installation

Ensure you have Rust installed (version 1.78.0 or later). If not, install it from https://rustup.rs/.

1. Clone the repository:

2. Create a `.env` file in the project root and add your OpenAI API key:
```bash
OPENAI_API_KEY=your_api_key_here
```

3. Build the project:
```bash
cargo build --release
```

## Usage

Start the server:
```bash
cargo run --release
```

The server will start on `http://0.0.0.0:3001`. You can now send requests to the API endpoints.

## API Endpoints

### POST /sheets/analyze
Analyze an Excel file

Request body should include:
- `user_email`: String
- `chat_id`: String
- `messages`: Array of strings
- `files`: Array of file information objects (including type and signed_url)

## Configuration

The application uses environment variables for configuration. Make sure to set the following:

- `OPENAI_API_KEY`: Your OpenAI API key for AI-powered analysis

You can adjust other configuration options in the Config struct within `src/config.rs`.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a new branch: `git checkout -b feature-branch-name`
3. Make your changes and commit them: `git commit -m 'Add some feature'`
4. Push to the branch: `git push origin feature-branch-name`
5. Submit a pull request

Please ensure your code adheres to the existing style and passes all tests.


## License

This project is licensed under the [MIT License](LICENSE).

## Acknowledgements

- [Axum](https://github.com/tokio-rs/axum) for the web framework
- [Tokio](https://tokio.rs/) for the asynchronous runtime
- [Polars](https://github.com/pola-rs/polars) for data processing
- [OpenAI](https://openai.com/) for AI-powered analysis capabilities
```
