"""
Config Generator Framework
Generates config.yml from CSV/Excel input file
"""

import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys


class ConfigGenerator:
    """Framework to generate config.yml from CSV/Excel input"""

    DOMAIN_FIELDS = [
        'domain_name',
        'data_product_name', 
        'owner_team',
        'owner_email',
        'steward'
    ]
    
    DATASET_FIELDS = [
        'dataset_name',
        'dataset_version',
        'dataset_type',
        'data_tier',
        'business_description',
        'criticality',
        'data_classification',
        'contains_pii',
        'regulatory_flags',
        'cost_center',
        'lifecycle_status',
        'retention_days',
        'is_active',
        'file_name',
        'schema',
        'frequency',
        'environment',
        'source_type',
        'source_system',
        'ingestion_mode',
        'load_type',
        'source_bucket',
        'source_path',
        'file_format',
        'delimiter',
        'encoding',
        'primary_keys',
        'watermark_column',
        'checkpoint_location',
        'schema_location',
        'tag_key_1',
        'tag_value_1',
        'tag_key_2',
        'tag_value_2',
        'dq_enabled',
        'fail_action',
        'alert_channel',
        'escalation_contact'
    ]
    
    def __init__(self):
        self.domain_config = {}
        self.datasets = []
    
    def create_template(self, output_path: str, format: str = 'excel'):
        """
        Create a template CSV/Excel file with all required fields
        
        Args:
            output_path: Path where template file will be created
            format: 'excel' or 'csv'
        """
        domain_df = pd.DataFrame({
            'field_name': self.DOMAIN_FIELDS,
            'value': ['', '', '', '', ''],
            'description': [
                'Domain name (e.g., finance)',
                'Data product name',
                'Owning team name',
                'Owner email address',
                'Data steward name'
            ],
            'example': [
                'finance',
                'finance',
                'ga_analytics',
                'gparimi@exelixis.com',
                'Gagan Parimi'
            ]
        })
        
        dataset_df = pd.DataFrame(columns=self.DATASET_FIELDS)

        descriptions = {
            'dataset_name': 'Unique name for the dataset',
            'dataset_version': 'Version (e.g., v1)',
            'dataset_type': 'Type: source/derived/curated',
            'data_tier': 'Tier: raw/bronze/silver/gold',
            'business_description': 'Business description of dataset',
            'criticality': 'Criticality: low/medium/high/critical',
            'data_classification': 'Classification: public/internal/confidential',
            'contains_pii': 'Contains PII: true/false',
            'regulatory_flags': 'Regulatory compliance flags',
            'cost_center': 'Associated cost center',
            'lifecycle_status': 'Status: active/deprecated/archived',
            'retention_days': 'Retention period in days',
            'is_active': 'Is active: true/false',
            'file_name': 'Source file name',
            'schema': 'Schema name or None',
            'frequency': 'Update frequency: daily/weekly/monthly',
            'environment': 'Environment: dev/qa/prod',
            'source_type': 'Source type or null',
            'source_system': 'Source system name',
            'ingestion_mode': 'Mode: batch/streaming',
            'load_type': 'Load type: full_load/incremental',
            'source_bucket': 'Source bucket (use ${SOURCE_BUCKET} for variable)',
            'source_path': 'Path within source bucket',
            'file_format': 'Format: csv/parquet/json/delta',
            'delimiter': 'Delimiter for CSV (e.g., ,)',
            'encoding': 'File encoding (e.g., UTF-8)',
            'primary_keys': 'Comma-separated primary keys or blank',
            'watermark_column': 'Column for watermarking or null',
            'checkpoint_location': 'Checkpoint location path',
            'schema_location': 'Schema location path',
            'tag_key_1': 'First tag key',
            'tag_value_1': 'First tag value',
            'tag_key_2': 'Second tag key',
            'tag_value_2': 'Second tag value',
            'dq_enabled': 'Data quality enabled: true/false',
            'fail_action': 'Failure action: fail_pipeline/warn/skip',
            'alert_channel': 'Alert channel: email/slack/teams',
            'escalation_contact': 'Escalation contact email'
        }
        
        examples = {
            'dataset_name': 'metadata_years',
            'dataset_version': 'v1',
            'dataset_type': 'source',
            'data_tier': 'raw',
            'business_description': 'Finance years data',
            'criticality': 'high',
            'data_classification': 'internal',
            'contains_pii': 'false',
            'regulatory_flags': 'null',
            'cost_center': 'null',
            'lifecycle_status': 'active',
            'retention_days': '3650',
            'is_active': 'true',
            'file_name': 'ExportedMetadata_Years',
            'schema': 'None',
            'frequency': 'daily',
            'environment': 'dev',
            'source_type': 'null',
            'source_system': 'oracle',
            'ingestion_mode': 'batch',
            'load_type': 'full_load',
            'source_bucket': '${SOURCE_BUCKET}',
            'source_path': 'ga/finance/oracle_finance/dev/',
            'file_format': 'csv',
            'delimiter': ',',
            'encoding': 'UTF-8',
            'primary_keys': '',
            'watermark_column': 'null',
            'checkpoint_location': '/Volumes/${METADATA_CATALOG}/${METADATA_SCHEMA}/checkpoints/metadata_years',
            'schema_location': '/Volumes/${METADATA_CATALOG}/${METADATA_SCHEMA}/schemas/metadata_years',
            'tag_key_1': 'domain',
            'tag_value_1': 'finance',
            'tag_key_2': 'project',
            'tag_value_2': 'finance',
            'dq_enabled': 'true',
            'fail_action': 'fail_pipeline',
            'alert_channel': 'email',
            'escalation_contact': 'gparimi@exelixis.com'
        }
        
        if format.lower() == 'excel':
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                domain_df.to_excel(writer, sheet_name='Domain_Info', index=False)

                dataset_template = pd.DataFrame([descriptions, examples])
                dataset_template.to_excel(writer, sheet_name='Datasets', index=False)

                instructions = pd.DataFrame({
                    'Instructions': [
                        '1. Fill in the Domain_Info sheet with your domain configuration',
                        '2. Fill in the Datasets sheet with your dataset configurations',
                        '3. Add one row per dataset in the Datasets sheet',
                        '4. First row contains field descriptions, second row contains examples',
                        '5. Start adding your data from row 3 onwards',
                        '6. Use "true"/"false" for boolean values',
                        '7. Use "null" or leave blank for null values',
                        '8. For primary_keys, use comma-separated values (e.g., "id,name")',
                        '9. Save the file and run: python yaml_generator.py your_file.xlsx',
                        '10. This will generate config.yml in the same directory'
                    ]
                })
                instructions.to_excel(writer, sheet_name='Instructions', index=False)
        else:
            domain_df.to_csv(output_path.replace('.csv', '_domain.csv'), index=False)

            dataset_template = pd.DataFrame([descriptions, examples])
            dataset_template.to_csv(output_path.replace('.csv', '_datasets.csv'), index=False)
        
        print(f"✓ Template created: {output_path}")
        if format.lower() == 'csv':
            print(f"✓ Domain template: {output_path.replace('.csv', '_domain.csv')}")
            print(f"✓ Datasets template: {output_path.replace('.csv', '_datasets.csv')}")
    
    def _convert_value(self, value: Any, field_name: str) -> Any:
        """Convert string values to appropriate Python types"""
        if pd.isna(value) or value == '' or str(value).lower() == 'null':
            return None

        if str(value).lower() in ['true', 'false']:
            return str(value).lower() == 'true'

        if field_name == 'retention_days':
            return int(value)

        if field_name == 'primary_keys':
            if value:
                return [k.strip() for k in str(value).split(',') if k.strip()]
            return []
        
        return value
    
    def load_from_file(self, input_path: str):
        """
        Load configuration from CSV/Excel file
        
        Args:
            input_path: Path to the input file
        """
        file_path = Path(input_path)

        if file_path.suffix.lower() in ['.xlsx', '.xls']:
            domain_df = pd.read_excel(input_path, sheet_name='Domain_Info')
            datasets_df = pd.read_excel(input_path, sheet_name='Datasets')
        elif file_path.suffix.lower() == '.csv':
            domain_path = str(file_path).replace('.csv', '_domain.csv')
            datasets_path = str(file_path).replace('.csv', '_datasets.csv')
            domain_df = pd.read_csv(domain_path)
            datasets_df = pd.read_csv(datasets_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")

        for _, row in domain_df.iterrows():
            field_name = row['field_name']
            value = row['value']
            if pd.notna(value) and value != '':
                self.domain_config[field_name] = value

        for idx, row in datasets_df.iterrows():
            if idx < 1:
                continue

            if pd.isna(row.get('dataset_name')) or row.get('dataset_name') == '':
                continue

            dataset = {}

            for field in self.DATASET_FIELDS:
                if field in row:
                    value = self._convert_value(row[field], field)

                    if field.startswith('tag_'):
                        continue
                    elif field in ['dq_enabled', 'fail_action', 'alert_channel', 'escalation_contact']:
                        continue
                    else:
                        dataset[field] = value

            tags = []
            if pd.notna(row.get('tag_key_1')) and row.get('tag_key_1') != '':
                tags.append({
                    'key': row['tag_key_1'],
                    'value': row.get('tag_value_1', '')
                })
            if pd.notna(row.get('tag_key_2')) and row.get('tag_key_2') != '':
                tags.append({
                    'key': row['tag_key_2'],
                    'value': row.get('tag_value_2', '')
                })
            
            if tags:
                dataset['tags'] = tags

            if pd.notna(row.get('dq_enabled')):
                dq_rule = {
                    'rule_set_name': '',
                    'dq_enabled': self._convert_value(row.get('dq_enabled'), 'dq_enabled'),
                    'fail_action': row.get('fail_action', 'fail_pipeline'),
                    'alert_channel': row.get('alert_channel', 'email'),
                    'escalation_contact': row.get('escalation_contact', '')
                }
                dataset['dq_rules'] = [dq_rule]
            
            self.datasets.append(dataset)
        
        print(f"✓ Loaded configuration from {input_path}")
        print(f"  - Domain: {self.domain_config.get('domain_name')}")
        print(f"  - Datasets: {len(self.datasets)}")
    
    def generate_config_yml(self, output_path: str):
        """
        Generate config.yml file
        
        Args:
            output_path: Path where config.yml will be created
        """
        config = {
            **self.domain_config,
            'datasets': self.datasets
        }
        
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        
        print(f"✓ Generated config.yml: {output_path}")


def main():
    """Main entry point for the config generator"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Config Generator Framework - Generate config.yml from CSV/Excel',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python yaml_generator.py --create-template config_template.xlsx

  python yaml_generator.py --create-template config_template.csv --format csv

  python yaml_generator.py --input config_template.xlsx --output config.yml

  python yaml_generator.py --input config_template.csv --output config.yml --format csv
        """
    )
    
    parser.add_argument(
        '--create-template',
        type=str,
        help='Create a template file at the specified path'
    )
    
    parser.add_argument(
        '--input',
        type=str,
        help='Input CSV/Excel file with configuration data'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        default='config.yml',
        help='Output config.yml file path (default: config.yml)'
    )
    
    parser.add_argument(
        '--format',
        type=str,
        choices=['excel', 'csv'],
        default='excel',
        help='File format: excel or csv (default: excel)'
    )
    
    args = parser.parse_args()
    
    generator = ConfigGenerator()
    
    if args.create_template:
        generator.create_template(args.create_template, args.format)
    elif args.input:
        generator.load_from_file(args.input)
        generator.generate_config_yml(args.output)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
