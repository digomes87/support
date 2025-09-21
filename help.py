from datetime import datetime, timedelta
from typing import Dict, Any

from boto3 import Session
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from dateutil import tz
from utils.logging_config import ETLLogger


class RefreshableBotoSession:
    def __init__(
        self,
        region_name: str = "sa-east-1",
        profile_name: str = None,
        sts_arn: str = None,
        session_name: str = None,
        session_ttl: int = 300,
    ):
        self.region_name = region_name
        self.profile_name = profile_name
        self.sts_arn = sts_arn
        self.session_name = session_name or f"session-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        self.session_ttl = session_ttl
        self.logger = ETLLogger(__name__)

    def __get_session_credentials(self) -> Dict[str, Any]:
        """Get session credentials either from STS assume role or current session."""
        try:
            session = Session(region_name=self.region_name, profile_name=self.profile_name)

            if self.sts_arn:
                self.logger.info(f"Assuming role: {self.sts_arn}")
                sts_client = session.client(
                    service_name="sts", region_name=self.region_name
                )
                response = sts_client.assume_role(
                    RoleArn=self.sts_arn,
                    RoleSessionName=self.session_name,
                    DurationSeconds=self.session_ttl,
                ).get("Credentials")

                utc_exp: datetime = response.get("Expiration")
                credentials = {
                    "access_key": response.get("AccessKeyId"),
                    "secret_key": response.get("SecretAccessKey"),
                    "token": response.get("SessionToken"),
                    "expire_time": utc_exp.astimezone(tz.tzlocal()).isoformat(),
                }
            else:
                self.logger.info("Using current session credentials")
                session_credentials = session.get_credentials()

                utc_exp = datetime.utcnow() + timedelta(seconds=self.session_ttl)
                utc_exp = utc_exp.replace(tzinfo=tz.UTC)

                credentials = {
                    "access_key": session_credentials.access_key,
                    "secret_key": session_credentials.secret_key,
                    "token": session_credentials.token,
                    "expire_time": utc_exp.astimezone(tz.tzlocal()).isoformat(),
                }
            
            return credentials
            
        except Exception as e:
            self.logger.error(f"Error getting session credentials: {str(e)}")
            raise

    def refreshable_session(self) -> Session:
        """Create a refreshable boto3 session with automatic credential refresh."""
        try:
            self.logger.info("Creating refreshable session")
            refreshable_credentials = RefreshableCredentials.create_from_metadata(
                metadata=self.__get_session_credentials(),
                refresh_using=self.__get_session_credentials,
                method="sts-assume-role",
            )
            session = get_session()
            session._credentials = refreshable_credentials
            session.set_config_variable("region", self.region_name)
            autorefresh_session = Session(botocore_session=session)

            self.logger.info("Refreshable session created successfully")
            return autorefresh_session
            
        except Exception as e:
            self.logger.error(f"Error creating refreshable session: {str(e)}")
            raise
