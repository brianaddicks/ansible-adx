#!/usr/bin/python

# Copyright: (c) 2023, Brian Addicks <brian@addicks.us>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r"""
---
module: ingest

short_description: Ingest local json file into Azure Data Explorer table.

# If this is part of a collection, you need to use semantic versioning,
# i.e. the version is of the form "2.5.0" and not "2.4".
version_added: "1.0.0"

description: Ingest local json file into Azure Data Explorer table.

options:
    cluster_uri:
        description:
            - ADX Cluster URI
            - 'ie: https://myadx.region.kusto.windows.net'
            - Required if environment variable ADX_CLUSTER_URI is not set
        required: false
        type: str
    cluster_ingest_uri:
        description:
            - ADX Cluster URI
            - 'ie: https://ingest-myadx.region.kusto.windows.net'
            - Required if environment variable ADX_CLUSTER_INGEST_URI is not set
        required: false
        type: str
    client_id:
        description:
            - Client ID used to authenticate ot adx
            - 'ie: aaaabbbb-cccc-dddd-1111-222233334444'
            - Required if environment variable ADX_CLIENT_ID is not set
        required: false
        type: str
    client_secret:
        description:
            - Secret associated with Client ID
            - Required if environment variables ADX_CLIENT_SECRET is not set
        required: false
        type: str
    tenant_id:
        description:
            - Azure Tenant ID
            - 'ie: contoso.onmicrosoft.com'
            - Required if environment variables ADX_TENANT_ID is not set
        required: false
        type: str
    database_name:
        description:
            - ADX database name
        required: true
        type: str
    table_name:
        description:
            - ADX table name
        required: true
        type: str
    ingest_mapping_name:
        description:
            - Name of existing ingest mapping
        required: true
        type: str
    table_schema:
        description:
            - ADX table schema
            - 'ie: (MyColumn:datetime, MyColumn2: dynamic)'
        required: true
        type: str
    json_file:
        description:
            - path to json file to import
        required: true
        type: str
# Specify this value according to your collection
# in format of namespace.collection.doc_fragment_name
# extends_documentation_fragment:
#     - my_namespace.my_collection.my_doc_fragment_name

author:
    - Brian Addicks (@brianaddicks)
"""

EXAMPLES = r"""
# Pass in a message
- name: Ingest data to ADX
  ingest:
    cluster_ingest_uri: https://ingest-myadx.region.kusto.windows.net
    client_id: aaaabbbb-cccc-dddd-1111-222233334444
    client_secret: '{{ my_super_secret }}'
    tenant_id: contoso.onmicrosoft.com
    database_name: testdb
    table_name: testtable
    ingest_mapping_name: testtable_mapping
    cluster_uri: https://myadx.region.kusto.windows.net
    json_file: ./ingest.json
    table_schema: "(MyColumn:datetime, MyColumn2: dynamic)"

"""

from ansible.module_utils.basic import AnsibleModule, env_fallback
import dataclasses
import enum
import json
import uuid
from dataclasses import dataclass
from typing import List
import inflection as inflection
import os
from time import sleep
from tqdm import tqdm
from azure.kusto.data import (
    KustoConnectionStringBuilder,
    ClientRequestProperties,
    KustoClient,
    DataFormat,
)
from azure.kusto.data.exceptions import KustoClientError, KustoServiceError
from azure.kusto.ingest import (
    IngestionProperties,
    BaseIngestClient,
    QueuedIngestClient,
    FileDescriptor,
    BlobDescriptor,
)


class AuthenticationModeOptions(enum.Enum):
    """
    AuthenticationModeOptions - represents the different options to autenticate to the system
    """

    UserPrompt = ("UserPrompt",)
    ManagedIdentity = ("ManagedIdentity",)
    AppKey = ("AppKey",)
    AppCertificate = "AppCertificate"


class Utils:
    class Authentication:
        """
        Authentication module of Utils - in charge of authenticating the user with the system
        """

        @classmethod
        def generate_connection_string(
            cls,
            cluster_url: str,
            authentication_mode: AuthenticationModeOptions,
            client_id="",
            client_secret="",
            tenant_id="",
        ) -> KustoConnectionStringBuilder:
            """
            Generates Kusto Connection String based on given Authentication Mode.
            :param cluster_url: Cluster to connect to.
            :param authentication_mode: User Authentication Mode, Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)
            :return: A connection string to be used when creating a Client
            """
            # Learn More: For additional information on how to authorize users and apps in Kusto,
            # see: https://docs.microsoft.com/azure/data-explorer/manage-database-permissions

            if authentication_mode == AuthenticationModeOptions.UserPrompt.name:
                # Prompt user for credentials
                return KustoConnectionStringBuilder.with_interactive_login(cluster_url)

            elif authentication_mode == AuthenticationModeOptions.ManagedIdentity.name:
                # Authenticate using a System-Assigned managed identity provided to an azure service, or using a User-Assigned managed identity.
                # For more information, see https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
                return cls.create_managed_identity_connection_string(cluster_url)

            elif authentication_mode == AuthenticationModeOptions.AppKey.name:
                # Learn More: For information about how to procure an AAD Application,
                # see: https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
                # TODO (config - optional): App ID & tenant, and App Key to authenticate with
                return KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    cluster_url, client_id, client_secret, tenant_id
                )

            elif authentication_mode == AuthenticationModeOptions.AppCertificate.name:
                return cls.create_application_certificate_connection_string(cluster_url)

            else:
                Utils.error_handler(
                    f"Authentication mode '{authentication_mode}' is not supported"
                )

        @classmethod
        def create_managed_identity_connection_string(
            cls, cluster_url: str
        ) -> KustoConnectionStringBuilder:
            """
            Generates Kusto Connection String based on 'ManagedIdentity' Authentication Mode.
            :param cluster_url: Url of cluster to connect to
            :return: ManagedIdentity Kusto Connection String
            """
            # Connect using the system- or user-assigned managed identity (Azure service only)
            # TODO (config - optional): Managed identity client ID if you are using a user-assigned managed identity
            client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
            return (
                KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(
                    cluster_url, client_id=client_id
                )
                if client_id
                else KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(
                    cluster_url
                )
            )

        @classmethod
        def create_application_certificate_connection_string(
            cls, cluster_url: str
        ) -> KustoConnectionStringBuilder:
            """
            Generates Kusto Connection String based on 'AppCertificate' Authentication Mode.
            :param cluster_url: Url of cluster to connect to
            :return: AppCertificate Kusto Connection String
            """

            # TODO (config - optional): App ID & tenant, path to public certificate and path to private certificate pem file to authenticate with
            app_id = os.environ.get("APP_ID")
            app_tenant = os.environ.get("APP_TENANT")
            private_key_pem_file_path = os.environ.get("PRIVATE_KEY_PEM_FILE_PATH")
            cert_thumbprint = os.environ.get("CERT_THUMBPRINT")
            public_cert_file_path = os.environ.get(
                "PUBLIC_CERT_FILE_PATH"
            )  # Only used for "Subject Name and Issuer" auth
            public_certificate = None
            pem_certificate = None

            try:
                with open(private_key_pem_file_path, "r") as pem_file:
                    pem_certificate = pem_file.read()
            except Exception as ex:
                Utils.error_handler(
                    f"Failed to load PEM file from {private_key_pem_file_path}", ex
                )

            if public_cert_file_path:
                try:
                    with open(public_cert_file_path, "r") as cert_file:
                        public_certificate = cert_file.read()
                except Exception as ex:
                    Utils.error_handler(
                        f"Failed to load public certificate file from {public_cert_file_path}",
                        ex,
                    )

                return KustoConnectionStringBuilder.with_aad_application_certificate_sni_authentication(
                    cluster_url,
                    app_id,
                    pem_certificate,
                    public_certificate,
                    cert_thumbprint,
                    app_tenant,
                )
            else:
                return KustoConnectionStringBuilder.with_aad_application_certificate_authentication(
                    cluster_url, app_id, pem_certificate, cert_thumbprint, app_tenant
                )

    class Queries:
        """
        Queries module of Utils - in charge of querying the data - either with management queries, or data queries
        """

        MGMT_PREFIX = "."

        @classmethod
        def create_client_request_properties(
            cls, scope: str, timeout: str = None
        ) -> ClientRequestProperties:
            """
            Creates a fitting ClientRequestProperties object, to be used when executing control commands or queries.
            :param scope: Working scope
            :param timeout: Requests default timeout
            :return: ClientRequestProperties object
            """
            client_request_properties = ClientRequestProperties()
            client_request_properties.client_request_id = f"{scope};{str(uuid.uuid4())}"
            client_request_properties.application = "sample_app.py"

            # Tip: Though uncommon, you can alter the request default command timeout using the below command, e.g. to set the timeout to 10 minutes, use "10m"
            if timeout:
                client_request_properties.set_option(
                    ClientRequestProperties.request_timeout_option_name, timeout
                )

            return client_request_properties

        @classmethod
        def execute_command(
            cls, kusto_client: KustoClient, database_name: str, command: str
        ) -> bool:
            """
            Executes a Command using a premade client
            :param kusto_client: Premade client to run Commands. can be either an adminClient or queryClient
            :param database_name: DB name
            :param command: The Command to execute
            :return: True on success, false otherwise
            """
            try:
                if command.startswith(cls.MGMT_PREFIX):
                    client_request_properties = cls.create_client_request_properties(
                        "Python_SampleApp_ControlCommand"
                    )
                else:
                    client_request_properties = cls.create_client_request_properties(
                        "Python_SampleApp_Query"
                    )

                result = kusto_client.execute(
                    database_name, command, client_request_properties
                )
                print(f"Response from executed command '{command}':")
                for row in result.primary_results[0]:
                    print(row.to_list())

                return True

            except KustoClientError as ex:
                Utils.error_handler(
                    f"Client error while trying to execute command '{command}' on database '{database_name}'",
                    ex,
                )
            except KustoServiceError as ex:
                Utils.error_handler(
                    f"Server error while trying to execute command '{command}' on database '{database_name}'",
                    ex,
                )
            except Exception as ex:
                Utils.error_handler(
                    f"Unknown error while trying to execute command '{command}' on database '{database_name}'",
                    ex,
                )

            return False

    class Ingestion:
        """
        Ingestion module of Utils - in charge of ingesting the given data - based on the configuration file.
        """

        @classmethod
        def create_ingestion_properties(
            cls,
            database_name: str,
            table_name: str,
            data_format: DataFormat,
            mapping_name: str,
            ignore_first_record: bool,
        ) -> IngestionProperties:
            """
            Creates a fitting KustoIngestionProperties object, to be used when executing ingestion commands.
            :param database_name: DB name
            :param table_name: Table name
            :param data_format: Given data format
            :param mapping_name: Desired mapping name
            :param ignore_first_record: Flag noting whether to ignore the first record in the table. only relevant to
            tabular textual formats (CSV and the likes). for more information
            please read: https://docs.microsoft.com/en-us/azure/data-explorer/ingestion-properties
            :return: IngestionProperties object
            """
            return IngestionProperties(
                database=f"{database_name}",
                table=f"{table_name}",
                ingestion_mapping_reference=mapping_name,
                # Learn More: For more information about supported data formats, see: https://docs.microsoft.com/azure/data-explorer/ingestion-supported-formats
                data_format=data_format,
                # TODO (config - optional): Setting the ingestion batching policy takes up to 5 minutes to take effect.
                #  We therefore set Flush-Immediately for the sake of the sample, but it generally shouldn't be used in practice.
                #  Comment out the line below after running the sample the first few times.
                flush_immediately=True,
                ignore_first_record=ignore_first_record,
            )

        @classmethod
        def ingest_from_file(
            cls,
            ingest_client: BaseIngestClient,
            database_name: str,
            table_name: str,
            file_path: str,
            data_format: DataFormat,
            ignore_first_record,
            mapping_name: str = None,
        ) -> None:
            """
            Ingest Data from a given file path.
            :param ingest_client: Client to ingest data
            :param database_name: DB name
            :param table_name: Table name
            :param file_path: File path
            :param data_format: Given data format
            :param ignore_first_record: Flag noting whether to ignore the first record in the table. only relevant to
            tabular textual formats (CSV and the likes). for more information
            please read: https://docs.microsoft.com/en-us/azure/data-explorer/ingestion-properties
            :param mapping_name: Desired mapping name
            """
            ingestion_properties = cls.create_ingestion_properties(
                database_name,
                table_name,
                data_format,
                mapping_name,
                ignore_first_record,
            )

            # Tip 1: For optimal ingestion batching and performance,specify the uncompressed data size in the file descriptor instead of the default below of 0.
            # Otherwise, the service will determine the file size, requiring an additional s2s call, and may not be accurate for compressed files.
            # Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere
            file_descriptor = FileDescriptor(file_path, size=0, source_id=uuid.uuid4())
            ingest_client.ingest_from_file(
                file_descriptor, ingestion_properties=ingestion_properties
            )

        @classmethod
        def ingest_from_blob(
            cls,
            ingest_client: QueuedIngestClient,
            database_name: str,
            table_name: str,
            blob_url: str,
            data_format: DataFormat,
            ignore_first_record: bool,
            mapping_name: str = None,
        ) -> None:
            """
            Ingest Data from a Blob.
            :param ingest_client: Client to ingest data
            :param database_name: DB name
            :param table_name: Table name
            :param blob_url: Blob Uri
            :param data_format: Given data format
            :param ignore_first_record: Flag noting whether to ignore the first record in the table. only relevant to
            tabular textual formats (CSV and the likes). for more information
            please read: https://docs.microsoft.com/en-us/azure/data-explorer/ingestion-properties
            :param mapping_name: Desired mapping name
            """
            ingestion_properties = cls.create_ingestion_properties(
                database_name,
                table_name,
                data_format,
                mapping_name,
                ignore_first_record,
            )

            # Tip 1: For optimal ingestion batching and performance,specify the uncompressed data size in the file descriptor instead of the default below of 0.
            # Otherwise, the service will determine the file size, requiring an additional s2s call, and may not be accurate for compressed files.
            # Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere
            blob_descriptor = BlobDescriptor(
                blob_url, size=0, source_id=str(uuid.uuid4())
            )
            ingest_client.ingest_from_blob(
                blob_descriptor, ingestion_properties=ingestion_properties
            )

        @classmethod
        def wait_for_ingestion_to_complete(cls, wait_for_ingest_seconds: int) -> None:
            """
            Halts the program for WaitForIngestSeconds, allowing the queued ingestion process to complete.
            :param wait_for_ingest_seconds: Sleep time to allow for queued ingestion to complete.
            """
            print(
                f"Sleeping {wait_for_ingest_seconds} seconds for queued ingestion to complete. Note: This may take longer depending on the file size "
                f"and ingestion batching policy."
            )

            for x in tqdm(range(wait_for_ingest_seconds, 0, -1)):
                sleep(1)

    @staticmethod
    def error_handler(error: str, e: Exception = None) -> None:
        """
        Error handling function. Will mention the appropriate error message (and the exception itself if exists), and will quit the program.
        :param error: Appropriate error message received from calling function
        :param e: Thrown exception
        """
        print(f"Script failed with error: {error}")
        if e:
            print(f"Exception: {e}")

        exit(-1)


class SourceType(enum.Enum):
    """
    SourceType - represents the type of files used for ingestion
    """

    local_file_source = "localFileSource"
    blob_source = "blobSource"
    nosource = "nosource"


@dataclass
class ConfigData:
    """
    ConfigData object - represents a file from which to ingest
    """

    source_type: SourceType
    data_source_uri: str
    data_format: DataFormat
    use_existing_mapping: bool
    mapping_name: str
    mapping_value: str

    @staticmethod
    def from_json(json_dict: dict) -> "ConfigData":
        json_dict["sourceType"] = SourceType[
            inflection.underscore(json_dict.pop("sourceType"))
        ]
        json_dict["dataFormat"] = DataFormat[json_dict.pop("format").upper()]
        return data_class_from_json(json_dict, ConfigData)


@dataclass
class ConfigJson:
    """
    ConfigJson object - represents a cluster and DataBase connection configuration file.
    """

    use_existing_table: bool
    database_name: str
    table_name: str
    table_schema: str
    kusto_uri: str
    ingest_uri: str
    data_to_ingest: List[ConfigData]
    alter_table: bool
    query_data: bool
    ingest_data: bool
    authentication_mode: AuthenticationModeOptions
    wait_for_user: bool
    ignore_first_record: bool
    wait_for_ingest_seconds: bool
    batching_policy: str
    client_id: str
    client_secret: str
    tenant_id: str

    @staticmethod
    def from_json(json_dict: dict) -> "ConfigJson":
        json_dict["dataToIngest"] = [
            ConfigData.from_json(j) for j in json_dict.pop("data")
        ]
        return data_class_from_json(json_dict, ConfigJson)


def remove_extra_keys(json_dict: dict, data_type: type) -> dict:
    assert dataclasses.is_dataclass(data_type)
    field_names = [field.name for field in dataclasses.fields(data_type)]
    return {key: value for key, value in json_dict.items() if key in field_names}


def keys_to_snake_case(json_dict: dict) -> dict:
    return {inflection.underscore(key): val for (key, val) in json_dict.items()}


def data_class_from_json(json_dict: dict, data_type: type) -> dataclasses.dataclass:
    assert dataclasses.is_dataclass(data_type)
    all_keys = keys_to_snake_case(json_dict)
    config_json_keys = remove_extra_keys(all_keys, data_type)
    return data_type(**config_json_keys)


class KustoSampleApp:
    # TODO (config):
    #  If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details
    #  If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately
    CONFIG_FILE_NAME = "kusto_sample_config.json"

    __step = 1
    config = None

    @classmethod
    def load_configs(cls, config_info: dict) -> None:
        """
        Loads JSON configuration file, and sets the metadata in place.
        :param config_file_name: Configuration file path.
        """
        # try:
        # with open(config_file_name, "r") as config_file:
        #     json_dict = json.load(config_file)
        cls.config = ConfigJson.from_json(config_info)

        # except Exception as ex:
        #     Utils.error_handler(f"Couldn't read load config file from file '{config_file_name}'", ex)

    @classmethod
    def pre_ingestion_querying(
        cls, config: ConfigJson, kusto_client: KustoClient
    ) -> None:
        """
        First phase, pre ingestion - will reach the provided DB with several control commands and a query based on the configuration File.
        :param config: ConfigJson object containing the SampleApp configuration
        :param kusto_client: Client to run commands
        """
        if config.use_existing_table:
            if config.alter_table:
                # Tip: Usually table was originally created with a schema appropriate for the data being ingested, so this wouldn't be needed.
                # Learn More: For more information about altering table schemas,
                #  see: https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
                cls.wait_for_user_to_proceed(
                    f"Alter-merge existing table '{config.database_name}.{config.table_name}' to align with the provided schema"
                )
                cls.alter_merge_existing_table_to_provided_schema(
                    kusto_client,
                    config.database_name,
                    config.table_name,
                    config.table_schema,
                )
            if config.query_data:
                # Learn More: For more information about Kusto Query Language (KQL), see: https://docs.microsoft.com/azure/data-explorer/write-queries
                cls.wait_for_user_to_proceed(
                    f"Get existing row count in '{config.database_name}.{config.table_name}'"
                )
                cls.query_existing_number_of_rows(
                    kusto_client, config.database_name, config.table_name
                )
        else:
            # Tip: This is generally a one-time configuration
            # Learn More: For more information about creating tables, see: https://docs.microsoft.com/azure/data-explorer/one-click-table
            cls.wait_for_user_to_proceed(
                f"Create table '{config.database_name}.{config.table_name}'"
            )
            cls.create_new_table(
                kusto_client,
                config.database_name,
                config.table_name,
                config.table_schema,
            )

        # Learn More:
        # Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
        #  1) More than 1,000 files were queued for ingestion for the same table by the same user
        #  2) More than 1GB of data was queued for ingestion for the same table by the same user
        #  3) More than 5 minutes have passed since the first file was queued for ingestion for the same table by the same user
        # For more information about customizing  ingestion batching policy, see: https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy

        # TODO: Change if needed. Disabled to prevent an existing batching policy from being unintentionally changed
        if False and config.batching_policy:
            cls.wait_for_user_to_proceed(
                f"Alter the batching policy for table '{config.database_name}.{config.table_name}'"
            )
            cls.alter_batching_policy(
                kusto_client,
                config.database_name,
                config.table_name,
                config.batching_policy,
            )

    @classmethod
    def alter_merge_existing_table_to_provided_schema(
        cls,
        kusto_client: KustoClient,
        database_name: str,
        table_name: str,
        table_schema: str,
    ) -> None:
        """
        Alter-merges the given existing table to provided schema.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        :param table_schema: Table Schema
        """
        command = f".alter-merge table {table_name} {table_schema}"
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def query_existing_number_of_rows(
        cls, kusto_client: KustoClient, database_name: str, table_name: str
    ) -> None:
        """
        Queries the data on the existing number of rows.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        """
        command = f"{table_name} | count"
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def query_first_two_rows(
        cls, kusto_client: KustoClient, database_name: str, table_name: str
    ) -> None:
        """
        Queries the first two rows of the table.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        """
        command = f"{table_name} | take 2"
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def create_new_table(
        cls,
        kusto_client: KustoClient,
        database_name: str,
        table_name: str,
        table_schema: str,
    ) -> None:
        """
        Creates a new table.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        :param table_schema: Table Schema
        """
        command = f".create table {table_name} {table_schema}"
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def alter_batching_policy(
        cls,
        kusto_client: KustoClient,
        database_name: str,
        table_name: str,
        batching_policy: str,
    ) -> None:
        """
        Alters the batching policy based on BatchingPolicy in configuration.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        :param batching_policy: Ingestion batching policy
        """
        # Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and in this sample app,
        # we opt to modify the default ingestion policy to ingest data after at most 10 seconds.
        # Tip 2: This is generally a one-time configuration.
        # Tip 3: You can skip the batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient
        command = (
            f".alter table {table_name} policy ingestionbatching @'{batching_policy}'"
        )
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def ingestion(
        cls,
        config: ConfigJson,
        kusto_client: KustoClient,
        ingest_client: QueuedIngestClient,
    ) -> None:
        """
        Second phase - The ingestion process.
        :param config: ConfigJson object
        :param kusto_client: Client to run commands
        :param ingest_client: Client to ingest data
        """

        for data_file in config.data_to_ingest:
            # Tip: This is generally a one-time configuration.
            # Learn More: For more information about providing inline mappings and mapping references,
            # see: https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
            cls.create_ingestion_mappings(
                data_file.use_existing_mapping,
                kusto_client,
                config.database_name,
                config.table_name,
                data_file.mapping_name,
                data_file.mapping_value,
                data_file.data_format,
            )

            # Learn More: For more information about ingesting data to Kusto in Python, see: https://docs.microsoft.com/azure/data-explorer/python-ingest-data
            cls.ingest_data(
                data_file,
                data_file.data_format,
                ingest_client,
                config.database_name,
                config.table_name,
                data_file.mapping_name,
                config.ignore_first_record,
            )

        Utils.Ingestion.wait_for_ingestion_to_complete(config.wait_for_ingest_seconds)

    @classmethod
    def create_ingestion_mappings(
        cls,
        use_existing_mapping: bool,
        kusto_client: KustoClient,
        database_name: str,
        table_name: str,
        mapping_name: str,
        mapping_value: str,
        data_format: DataFormat,
    ) -> None:
        """
        Creates Ingestion Mappings (if required) based on given values.
        :param use_existing_mapping: Flag noting if we should the existing mapping or create a new one
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        :param mapping_name: Desired mapping name
        :param mapping_value: Values of the new mappings to create
        :param data_format: Given data format
        """
        if use_existing_mapping or not mapping_value:
            return

        ingestion_mapping_kind = data_format.ingestion_mapping_kind.value.lower()
        cls.wait_for_user_to_proceed(
            f"Create a '{ingestion_mapping_kind}' mapping reference named '{mapping_name}'"
        )

        mapping_name = (
            mapping_name
            if mapping_name
            else "DefaultQuickstartMapping" + str(uuid.UUID())[:5]
        )
        mapping_command = f".create-or-alter table {table_name} ingestion {ingestion_mapping_kind} mapping '{mapping_name}' '{mapping_value}'"
        Utils.Queries.execute_command(kusto_client, database_name, mapping_command)

    @classmethod
    def ingest_data(
        cls,
        data_file: ConfigData,
        data_format: DataFormat,
        ingest_client: QueuedIngestClient,
        database_name: str,
        table_name: str,
        mapping_name: str,
        ignore_first_record: bool,
    ) -> None:
        """
        Ingest data from given source.
        :param data_file: Given data source
        :param data_format: Given data format
        :param ingest_client: Client to ingest data
        :param database_name: DB name
        :param table_name: Table name
        :param mapping_name: Desired mapping name
        :param ignore_first_record: Flag noting whether to ignore the first record in the table
        """
        source_type = data_file.source_type
        source_uri = data_file.data_source_uri
        cls.wait_for_user_to_proceed(f"Ingest '{source_uri}' from '{source_type.name}'")

        # Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
        # If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
        data_format = (
            DataFormat.MULTIJSON if data_format == data_format.JSON else data_format
        )

        # Tip: Kusto's Python SDK can ingest data from files, blobs, open streams and pandas dataframes.
        # See the SDK's samples and the E2E tests in azure.kusto.ingest for additional references.
        # Note: No need to add "nosource" option as in that case the "ingestData" flag will be set to false, and it will be imppossible to reach this code segemnt anyway.
        if source_type == SourceType.local_file_source:
            Utils.Ingestion.ingest_from_file(
                ingest_client,
                database_name,
                table_name,
                source_uri,
                data_format,
                ignore_first_record,
                mapping_name,
            )
        elif source_type == SourceType.blob_source:
            Utils.Ingestion.ingest_from_blob(
                ingest_client,
                database_name,
                table_name,
                source_uri,
                data_format,
                ignore_first_record,
                mapping_name,
            )
        else:
            Utils.error_handler(
                f"Unknown source '{source_type}' for file '{source_uri}'"
            )

    @classmethod
    def post_ingestion_querying(
        cls,
        kusto_client: KustoClient,
        database_name: str,
        table_name: str,
        config_ingest_data: bool,
    ) -> None:
        """
        Third and final phase - simple queries to validate the script ran successfully.
        :param kusto_client: Client to run queries
        :param database_name: DB Name
        :param table_name: Table Name
        :param config_ingest_data: Flag noting whether any data was ingested by the script
        """
        optional_post_ingestion_message = (
            "post-ingestion " if config_ingest_data else ""
        )

        # Note: We poll here the ingestion's target table because monitoring successful ingestions is expensive and not recommended.
        #   Instead, the recommended ingestion monitoring approach is to monitor failures.
        # Learn more: https://docs.microsoft.com/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-status#tracking-ingestion-status-kustoqueuedingestclient
        #   and https://docs.microsoft.com/azure/data-explorer/using-diagnostic-logs
        cls.wait_for_user_to_proceed(
            f"Get {optional_post_ingestion_message}row count for '{database_name}.{table_name}':"
        )
        cls.query_existing_number_of_rows(kusto_client, database_name, table_name)

        cls.wait_for_user_to_proceed(
            f"Get {optional_post_ingestion_message}row count for '{database_name}.{table_name}':"
        )
        cls.query_first_two_rows(kusto_client, database_name, table_name)

    @classmethod
    def wait_for_user_to_proceed(cls, prompt_msg: str) -> None:
        """
        Handles UX on prompts and flow of program
        :param prompt_msg: Prompt to display to user
        """
        print()
        print(f"Step {cls.__step}: {prompt_msg}")
        cls.__step = cls.__step + 1
        if cls.config.wait_for_user:
            input("Press ENTER to proceed with this operation...")


def run_module():
    # define available arguments/parameters a user can pass to the module
    module_args = dict(
        cluster_ingest_uri=dict(
            required=False, fallback=(env_fallback, ["ADX_CLUSTER_INGEST_URI"])
        ),
        client_id=dict(required=False, fallback=(env_fallback, ["ADX_CLIENT_ID"])),
        client_secret=dict(
            no_log=True, required=False, fallback=(env_fallback, ["ADX_CLIENT_SECRET"])
        ),
        tenant_id=dict(required=False, fallback=(env_fallback, ["ADX_TENANT_ID"])),
        database_name=dict(required=True),
        table_name=dict(required=True),
        ingest_mapping_name=dict(required=False),
        cluster_uri=dict(
            required=False, fallback=(env_fallback, ["ADX_CLUSTER_URI"])
        ),
        table_schema=dict(required=True),
        json_file=dict(type="path", required=True),
    )

    # seed the result dict in the object
    # we primarily care about changed and state
    # changed is if this module effectively modified the target
    # state will include any data that you want your module to pass back
    # for consumption, for example, in a subsequent task
    result = dict(changed=False, original_message="", message="")

    # the AnsibleModule object will be our abstraction working with Ansible
    # this includes instantiation, a couple of common attr would be the
    # args/params passed to the execution, as well as if the module
    # supports check mode
    module = AnsibleModule(argument_spec=module_args, supports_check_mode=False)

    # load arguments into variables
    data_config = dict(
        sourceType="localFileSource",
        dataSourceUri=module.params.get("json_file"),
        format="multijson",
        useExistingMapping=True,
        mappingName=module.params.get("ingest_mapping_name"),
        mappingValue="",
    )

    batching_policy = dict(
        MaximumBathingTimeSpan="00:00:10",
        MaximumNumberOfItems=500,
        MaximumRawDataSizeMB=1024,
    )

    this_config = dict(
        authenticationMode="AppKey",
        waitForUser=False,
        waitForIngestSeconds=20,
        batchingPolicy=batching_policy,
        kustoUri=module.params.get("cluster_uri"),
        ingestUri=module.params.get("cluster_ingest_uri"),
        databaseName=module.params.get("database_name"),
        tableName=module.params.get("table_name"),
        useExistingTable=True,
        tableSchema=module.params.get("table_schema"),
        programmingLanguage="python",
        alterTable=True,
        queryData=True,
        ignoreFirstRecord=False,
        ingestData=True,
        data=[data_config],
        client_id=module.params.get("client_id"),
        client_secret=module.params.get("client_secret"),
        tenant_id=module.params.get("tenant_id"),
    )

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(this_config, f, ensure_ascii=False, indent=4)

    app = KustoSampleApp()
    app.load_configs(this_config)

    kusto_connection_string = Utils.Authentication.generate_connection_string(
        app.config.kusto_uri,
        app.config.authentication_mode,
        client_id=app.config.client_id,
        client_secret=app.config.client_secret,
        tenant_id=app.config.tenant_id,
    )
    ingest_connection_string = Utils.Authentication.generate_connection_string(
        app.config.ingest_uri,
        app.config.authentication_mode,
        client_id=app.config.client_id,
        client_secret=app.config.client_secret,
        tenant_id=app.config.tenant_id,
    )

    if not kusto_connection_string or not ingest_connection_string:
        Utils.error_handler(
            "Connection String error. Please validate your configuration file."
        )
    else:
        # Make sure to use context manager for the Kusto client, as it will close the underlying resources properly.
        # Alternatively, you can use the client as a regular Python object and call close() on it.
        with KustoClient(kusto_connection_string) as kusto_client:
            with QueuedIngestClient(ingest_connection_string) as ingest_client:
                app.pre_ingestion_querying(app.config, kusto_client)

                if app.config.ingest_data:
                    app.ingestion(app.config, kusto_client, ingest_client)

                if app.config.query_data:
                    app.post_ingestion_querying(
                        kusto_client,
                        app.config.database_name,
                        app.config.table_name,
                        app.config.ingest_data,
                    )

    # in the event of a successful module execution, you will want to
    # simple AnsibleModule.exit_json(), passing the key/value results
    result["changed"] = True
    module.exit_json(**result)


def main():
    run_module()


if __name__ == "__main__":
    main()
