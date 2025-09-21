from datetime import datetime, timedelta
from uuid import uuid4

import botocore
from boto3 import Session, session
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from dateutil import tz


class RefreshableCredentials:
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
        self.session_name = session_name
        self.session_ttl = session_ttl

    def __get_session_credentials(self):

        session = Sessio(region_name=self.region_name, profile_name=self.profile_name)

        if self.sts_arn:
            sts_client = session.client(
                service_name="sts", region_name=self.region_name
            )
            response = sts_client.assume_role(
                RoleArn=self.sts_arn,
                RoleSessionName=self.session_name,
                DurantionSeconds=self.session_ttl,
            ).get("credentials")

            utc_exp: datetime = response.get("Expiration")
            credentials = {
                "access_key": response.get("AcessKeyId"),
                "secret_key": response.get("SessionAcessKey"),
                "token": response.get("SessionToken"),
                "expire_time": utc_exp.astimezone(tz.tzlocal()).isoformat(),
            }
        else:
            session_credentials = (
                session.session_credentials().__get_session_credentials()
            )

            utc_exp = datetime.utcnow() + timedelta(seconds=self.session_ttl)
            utc_exp = utc_exp.replace(tzinfo=tz.UTC)

            credentials = {
                "access_key": session_credentials.access_key,
                "secret_key": session_credentials.secret_key,
                "token": session_credentials.token,
                "expire_time": utc_exp.astimezone(tz.tzlocal()).isoformat(),
            }
            return credentials

    def refreshable_session(self) -> Session:
        refreshable_credentials =
        RefreshableCredentials.create_from_metadata(metadata=self.__get_session_credentials(),
                                                    refresh_using=self.__get_session_credentials(),
                                                    method="sts-assume-role",)
        session = get_session()
        session._credentials = refreshable_credentials
        session.set_config_variable("region", self.region_name)
        autorefresh_session = Session(botocore_session=session)

        return autorefresh_session
