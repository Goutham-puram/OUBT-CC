# Data Engineering Internship 2024 - AWS Data Pipeline Project

A comprehensive 4-week AWS data engineering project using the NYC Yellow Taxi dataset to build a production-grade data pipeline from ingestion to analytics.

## Project Overview

This project demonstrates end-to-end data engineering on AWS, including:
- Data ingestion from public datasets
- Data lake architecture on S3
- ETL pipelines with AWS Glue
- Orchestration with Step Functions
- Data warehousing with Redshift
- Analytics and reporting

### Dataset
**NYC Yellow Taxi Trip Records** - Public dataset containing taxi trip data including pickup/dropoff times, locations, fares, and passenger counts.

## Project Timeline

### Week 1: Foundation & Database Setup
- AWS account setup and IAM configuration
- Python development environment with Boto3
- RDS PostgreSQL for metadata and initial data exploration
- Data ingestion scripts

### Week 2: Data Lake Architecture
- S3 data lake design (raw/processed/curated zones)
- AWS Glue Data Catalog setup
- Lambda functions for event-driven processing
- Data quality checks

### Week 3: ETL Pipeline Development
- AWS Glue ETL jobs (PySpark)
- Step Functions for workflow orchestration
- Data transformation and enrichment
- Automated pipeline scheduling

### Week 4: Data Warehouse & Analytics
- Redshift cluster setup and optimization
- Dimensional model design (star schema)
- Data loading into Redshift
- SQL analytics queries and views

## Project Structure

```
de-intern-2024-project/
├── infrastructure/          # Infrastructure as Code
│   ├── terraform/          # Terraform configurations
│   └── cloudformation/     # CloudFormation templates
├── src/                    # Source code
│   ├── de_intern_2024/    # Main Python package
│   │   ├── week1/         # Week 1 scripts
│   │   ├── week2/         # Week 2 scripts
│   │   ├── week3/         # Week 3 scripts
│   │   └── week4/         # Week 4 scripts
│   ├── lambda/            # Lambda function code
│   └── glue/              # Glue job scripts
├── sql/                   # SQL scripts
│   ├── migrations/        # Database migrations
│   └── queries/           # Analytics queries
├── tests/                 # Test suite
│   ├── unit/             # Unit tests
│   └── integration/      # Integration tests
├── docs/                  # Documentation
├── data/                  # Local data directory
│   ├── raw/              # Raw data files
│   ├── processed/        # Processed data
│   └── scripts/          # Data download scripts
├── config/               # Configuration files
└── .github/              # GitHub Actions workflows
    └── workflows/
```

## Prerequisites

- AWS Account with appropriate permissions
- Python 3.9+
- Terraform 1.0+
- AWS CLI configured
- PostgreSQL client (optional)

## Quick Start

### 1. Environment Setup

```bash
# Clone the repository
git clone <repository-url>
cd de-intern-2024-project

# Create virtual environment
make venv

# Activate virtual environment
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
make install
```

### 2. AWS Configuration

```bash
# Configure AWS credentials
aws configure

# Set your AWS region (recommended: us-east-1)
export AWS_DEFAULT_REGION=us-east-1
```

### 3. Infrastructure Deployment

```bash
# Initialize Terraform
cd infrastructure/terraform
terraform init

# Review the plan
terraform plan

# Deploy infrastructure
terraform apply
```

### 4. Run the Pipeline

```bash
# Download sample data
make download-data

# Run Week 1 scripts
make run-week1

# Run Week 2 scripts
make run-week2

# And so on...
```

## Development

### Running Tests

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run integration tests
make test-integration

# Run with coverage
make test-coverage
```

### Code Quality

```bash
# Format code
make format

# Lint code
make lint

# Type check
make type-check
```

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make venv` | Create virtual environment |
| `make install` | Install dependencies |
| `make install-dev` | Install dev dependencies |
| `make test` | Run all tests |
| `make lint` | Run linters |
| `make format` | Format code |
| `make clean` | Clean temporary files |
| `make deploy-infra` | Deploy infrastructure |
| `make download-data` | Download NYC Taxi data |

## AWS Resources Created

- **S3 Buckets**: Data lake storage (raw, processed, curated)
- **RDS PostgreSQL**: Metadata and operational database
- **Glue Catalog**: Data catalog and schema registry
- **Glue Jobs**: ETL processing jobs
- **Lambda Functions**: Event-driven processing
- **Step Functions**: Workflow orchestration
- **Redshift**: Data warehouse cluster
- **IAM Roles**: Service permissions
- **CloudWatch**: Logging and monitoring

## Cost Optimization

- Use AWS Free Tier where applicable
- RDS: db.t3.micro instance
- Redshift: Single-node dc2.large cluster
- S3: Lifecycle policies for data archival
- Remember to destroy resources after completion: `terraform destroy`

## Documentation

Detailed documentation for each week:
- [Week 1 Guide](docs/week1-guide.md)
- [Week 2 Guide](docs/week2-guide.md)
- [Week 3 Guide](docs/week3-guide.md)
- [Week 4 Guide](docs/week4-guide.md)

Additional resources:
- [Architecture Overview](docs/architecture.md)
- [Development Guide](docs/development.md)
- [Troubleshooting](docs/troubleshooting.md)

## Contributing

1. Create a feature branch
2. Make your changes
3. Run tests and linting
4. Submit a pull request

## License

MIT License - See LICENSE file for details

## Resources

- [NYC Taxi Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [AWS Documentation](https://docs.aws.amazon.com/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)

## Support

For questions or issues, please open a GitHub issue or contact the project maintainer.
