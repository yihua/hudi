/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
const { slackUrl } = require('./constants');
module.exports = {
    docs: [
        {
            type: 'category',
            label: 'Getting Started',
            collapsed: false,
            items: [
                'overview',
                'quick-start-guide',
                'flink-quick-start-guide',
                'python-rust-quick-start-guide',
                'docker_demo',
                'use_cases',
            ],
        },
        {
            type: 'category',
            label: 'Design & Concepts',
            items: [
                'hudi_stack',
                'timeline',
                'storage_layouts',
                'write_operations',
                'table_types',
                'key_generation',
                'record_payload',
                'metadata',
                'indexes',
                'concurrency_control',
                'schema_evolution',
            ],
        },
        {
            type: 'category',
            label: 'Ingestion',
            items: [
                'hoodie_streaming_ingestion',
                'ingestion_flink',
                'ingestion_kafka_connect',
            ],
        },
        {
            type: 'category',
            label: 'Writing Tables',
            items: [
                'sql_ddl',
                'sql_dml',
                'writing_data',
                'writing_tables_streaming_writes',
            ],
        },
        {
            type: 'category',
            label: 'Reading Tables',
            items: [
                'sql_queries',
                'reading_tables_batch_reads',
                'reading_tables_streaming_reads',
            ],
        },
        {
            type: 'category',
            label: 'Table Services',
            items: [
                'cleaning',
                'compaction',
                'clustering',
                'metadata_indexing',
                'rollbacks',
                'markers',
                'file_sizing',
                {
                    type: 'category',
                    label: 'Syncing to Catalogs',
                    items: [
                         'syncing_aws_glue_data_catalog',
                         'syncing_datahub',
                         'syncing_metastore',
                         'gcp_bigquery',
                         'syncing_xtable'
                    ],
                }
            ],
        },
        {
            type: 'category',
            label: 'Platform & Tools',
            items: [
                'snapshot_exporter',
                'precommit_validator',
                'platform_services_post_commit_callback',
                'disaster_recovery',
                'migration_guide',
            ],
        },
        {
            type: 'category',
            label: 'Operating Hudi',
            items: [
                'performance',
                'deployment',
                'procedures',
                'cli',
                'metrics',
                'encryption',
                'troubleshooting',
                'tuning-guide',
                'flink_tuning',
            ],
        },
        {
            type: 'category',
            label: 'Configurations',
            items: [
                'basic_configurations',
                'configurations',
                {
                    type: 'category',
                    label: 'Storage Configurations',
                    items: [
                        'cloud',
                        's3_hoodie',
                        'gcs_hoodie',
                        'oss_hoodie',
                        'azure_hoodie',
                        'cos_hoodie',
                        'ibm_cos_hoodie',
                        'bos_hoodie',
                        'jfs_hoodie',
                        'oci_hoodie'
                    ],
                },
            ],
        },
        {
            type: 'category',
            label: 'Frequently Asked Questions(FAQs)',
            items: [
                'faq',
                'faq_general',
                'faq_design_and_concepts',
                'faq_writing_tables',
                'faq_reading_tables',
                'faq_table_services',
                'faq_storage',
                'faq_integrations',
            ],
        },
        'privacy',
    ],
    quick_links: [
        {
            type: 'link',
            label: 'Powered By',
            href: 'powered-by',
        },
        {
            type: 'link',
            label: 'Chat with us on Slack',
            href: slackUrl
        },
    ],
};
