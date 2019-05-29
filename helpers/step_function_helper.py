
import boto3


class StepFunctionHelper:
    step_client = boto3.client('stepfunctions')

    def __init__(self):
        self.arn = ''

    def get_step_function_state_machines(self):
        """
        Getting all the state machines in the step functions
        :return: LIst of state machines in step functions
        """
        try:
            self.response = self.step_client.list_state_machines()
            state_machines = []
            for idx, i in enumerate(self.response['stateMachines']):
                state_machines.append(self.response['stateMachines'][idx]['name'])
                return state_machines
        except Exception as e:
            print(e)

    def start_execution(self,state_machine):
        """
        Starting the execution of the state machine
        :param state_machine: The name of the state machine.
        """
        try:
            self.response = self.step_client.list_state_machines()
            for idx, i in enumerate(self.response['stateMachines']):
                if self.response['stateMachines'][idx]['name'] == state_machine:
                    self.arn = self.response['stateMachines'][idx]['stateMachineArn']
                    break
            self.response_execution = self.step_client.start_execution(self.arn)
        except Exception as e:
            print(e)

    def get_status(self,state_machine):
        """
        To get the status of the latest state machine execution.
        :param state_machine: The name of the state machine.
        """
        try:
            self.response = self.step_client.list_state_machines()
            for idx, i in enumerate(self.response['stateMachines']):
                if self.response['stateMachines'][idx]['name'] == state_machine:
                    self.arn = self.response['stateMachines'][idx]['stateMachineArn']
                    break
            self.responce_status = self.step_client.list_executions(stateMachineArn=self.arn)
            self.status = self.responce_status['executions'][0].get('status')
            print("The status of the latest execution of "+state_machine+" state machine is "+ "'"+self.status+"'")
        except Exception as e:
            print(e)
