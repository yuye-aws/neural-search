import json
from ts.torch_handler.base_handler import BaseHandler
import torch
import os
from transformers import AutoTokenizer, AutoModel
from sagemaker_inference import logging
from ts.handler_utils.micro_batching import MicroBatching


class SparseEncodingModelHandler(BaseHandler):
    def __init__(self):
        super().__init__()
        self.tokenizer = None
        self.all_tokens = None
        self.initialized = False

    def initialize(self, context):
        self.manifest = context.manifest

        properties = context.system_properties
        self.device = torch.device("cuda:" + str(properties.get("gpu_id")) if torch.cuda.is_available() else "cpu")
        # Read model serialize/pt file
        self.tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/msmarco-distilbert-base-tas-b")
        self.model = AutoModel.from_pretrained("sentence-transformers/msmarco-distilbert-base-tas-b")
        self.model = self.model.to(self.device)
        print("finish load")

    def preprocess(self, requests):
        print("in preprocess")
        inputSentence = []

        batch_idx = []
        for request in requests:

            request_body = request.get("body")
            if isinstance(request_body, bytearray):
                request_body = request_body.decode('utf-8')
                request_body = json.loads((request_body))

            if isinstance(request_body, list):
                inputSentence += request_body
                batch_idx.append(len(request_body))
            else:
                inputSentence.append(request_body)
                batch_idx.append(1)
        print("batch is", batch_idx)
        input_data = self.tokenizer(inputSentence, return_tensors="pt", padding=True, add_special_tokens=True,
                                    max_length=128, return_token_type_ids=False,
                                    truncation="longest_first", return_attention_mask=True)


        input_data = input_data.to(self.device)
        return {"input": input_data, "batch_l": batch_idx}

    def inference(self, data, *args, **kwargs):
        batch_idx = data["batch_l"]
        data = data["input"]
        with torch.cuda.amp.autocast(),torch.no_grad():
            predictions = self. model(**data, return_dict=True)
        return {"pred": predictions.last_hidden_state[:,0], "batch_l": batch_idx}

    def postprocess(self, prediction):
        batch_idx = prediction["batch_l"]
        predictions = prediction["pred"]
        outputs = []
        #print(predictions.shape)
        index = 0
        for b in batch_idx:
            outputs.append(predictions[index:index + b, :].cpu().tolist())
            index += b
        return outputs

    def postResult(self, result_list):
        outputs = []
        for result in result_list:
            outputs.append(json.dumps({"name": "sentence_embedding",
                            "data_type": "FLOAT32",
                            "shape": len(result),
                            "data": result}))
        return outputs

    def handle(self, data, context):
        model_input = self.preprocess(data)
        model_output = self.inference(model_input)
        return self.postprocess(model_output)