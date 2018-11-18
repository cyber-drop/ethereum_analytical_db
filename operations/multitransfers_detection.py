from clients.custom_clickhouse import CustomClickhouse
from config import INDICES, MULTITRANSFERS_DETECTION_MODEL_NAME, MULTITRANSFERS_TOP_ADDRESSES, MULTITRANSFERS_THRESHOLD
from sklearn.externals import joblib
import numpy as np

class ClickhouseMultitransfersDetection:
  def __init__(self, indices=INDICES):
    self.client = CustomClickhouse()
    self.indices = indices

  def _iterate_tokens(self):
    return self.client.iterate(
      index=self.indices["contract"],
      fields=["address"],
      query="WHERE has(standards, 'erc20')"
    )

  def _iterate_top_addresses(self, token, limit=MULTITRANSFERS_TOP_ADDRESSES):
    return self.client.iterate(
      index=self.indices["internal_transaction"],
      fields=["from AS address"],
      return_id=False,
      query="WHERE to = '{}' GROUP BY from ORDER BY count(*) DESC LIMIT {}".format(token, limit)
    )

  def _load_model(self, model_name=MULTITRANSFERS_DETECTION_MODEL_NAME):
    return joblib.load("{}.pkl".format(model_name))

  def _get_predictions(self, model, dataset, threshold=MULTITRANSFERS_THRESHOLD):
    predictions = model.predict_proba(dataset)
    max_predictions = np.max(predictions, axis=1)
    classes = [model.classes_[i] for i in np.argmax(predictions, axis=1)]
    return [
      {"type": classes[index], "probability": prediction}
      for index, prediction in enumerate(max_predictions)
      if prediction >= threshold
    ]

  def _save_classes(self, multitransfers):
    for i, multitransfer in enumerate(multitransfers):
      multitransfer["id"] = "{}.{}".format(
        multitransfer["token"],
        multitransfer["address"]
      )
    self.client.bulk_index(index=self.indices["multitransfer"], docs=multitransfers)