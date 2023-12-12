import boto3
# from pyyaml import yaml


"""BACKEND UTILITIES TO CONTROL LAMBDAS"""
class LambdaDeployer:
    """THESE RUN INSIDE A REST API THAT CONTROLS THE LAMBDAS"""
    def deploy_lambda(self, name, config: "yaml", code,):
        """
            Takes a lambda name, config variables and code and deploys the lambda, consuming from SNS.

            We need to define what format "CODE" is in.
        """

    def kill_lambda(self, name: str):
        """Takes a lambda name, and kills it"""

    def pause_lambda(self, name: str):
        """
        """

    def resume_lambda(self, name: str):
        """
        """

    def get_status(self, name: str):
        """
        """

    def get_all(self):
        """
        """
