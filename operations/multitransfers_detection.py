from clients.custom_clickhouse import CustomClickhouse
from config import INDICES, MULTITRANSFERS_DETECTION_MODEL_NAME, MULTITRANSFERS_TOP_ADDRESSES, MULTITRANSFERS_THRESHOLD
from sklearn.externals import joblib
import numpy as np
import pandas as pd

TRANSFER_EVENT_HEX = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

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

  def _get_events_per_transaction(self, token, addresses):
    events_per_transaction = [
      event["_source"]
      for event_list in self.client.iterate(
        index=self.indices["event"],
        fields=[
          "COUNT(*) AS events_count",
          "concat('0x', any(substring(topics[2], 27, 40))) AS distributor",
          "COUNT(DISTINCT(topics[2])) AS distributors_count"
        ],
        return_id=False,
        query="WHERE address = '{}' AND topics[1] = '{}' GROUP BY transactionHash".format(token, TRANSFER_EVENT_HEX)
      )
      for event in event_list
    ]
    result = pd.DataFrame(events_per_transaction)
    return result[(result["distributors_count"] == 1) & result["distributor"].isin(addresses)].groupby("distributor")["events_count"].mean()

  def _get_ethereum_senders(self, token, addresses):
    addresses_string = ",".join(["'{}'".format(address) for address in addresses])
    holders_ethereum_senders = [
      sender["_source"]
      for sender_list in self.client.iterate(
        index=self.indices["internal_transaction"],
        fields=[
          "to", "count(distinct(from)) AS ethereum_senders",
        ],
        return_id=False,
        query="""
          WHERE to in({})
          AND from in(
            SELECT distinct(concat('0x', substring(topics[3], 27, 40))) 
            FROM {}
            WHERE topics[1] = '{}'
            AND address = '{}'
          ) 
          GROUP BY to 
        """.format(addresses_string, self.indices["event"], TRANSFER_EVENT_HEX, token)
      )
      for sender in sender_list
    ]
    holders_ethereum_senders_df = pd.DataFrame(holders_ethereum_senders).set_index("to")["ethereum_senders"]
    total_ethereum_senders = [
      sender["_source"]
      for sender_list in self.client.iterate(
        index=self.indices["internal_transaction"],
        fields=[
          "to", "count(distinct(from)) AS ethereum_senders"
        ],
        return_id=False,
        query="WHERE to in({}) GROUP BY to".format(addresses_string)
      )
      for sender in sender_list
    ]
    total_ethereum_senders_df = pd.DataFrame(total_ethereum_senders).set_index("to")["ethereum_senders"]
    return holders_ethereum_senders_df / total_ethereum_senders_df

  def _get_total_token_receivers(self, token):
    return self.client.send_sql_request("""
      SELECT COUNT(DISTINCT(topics[3])) 
      FROM {} 
      WHERE address = '{}'
      AND topics[1] = '{}'
    """.format(self.indices["event"], token, TRANSFER_EVENT_HEX))

  def _get_token_receivers(self, token, addresses):
    addresses_string = ",".join(["'{}'".format(address) for address in addresses])
    token_receivers = [
      receiver["_source"]
      for receiver_list in self.client.iterate(
        index=self.indices["event"],
        fields=[
          "concat('0x', substring(topics[2], 27, 40)) AS from", "count(distinct(topics[3])) AS token_receivers"
        ],
        return_id=False,
        query="""
          WHERE concat('0x', substring(topics[2], 27, 40)) in({})
          AND address = '{}' 
          AND topics[1] = '{}'
          GROUP BY concat('0x', substring(topics[2], 27, 40))
        """.format(addresses_string, token, TRANSFER_EVENT_HEX)
      )
      for receiver in receiver_list
    ]
    token_receivers_df = pd.DataFrame(token_receivers).set_index("from")["token_receivers"]
    total_token_receivers = self._get_total_token_receivers(token)
    return token_receivers_df / total_token_receivers

  def _get_initiated_holders(self, token, addresses):
    addresses_string = ",".join(["'{}'".format(address) for address in addresses])
    initiated_holders = [
      holder["_source"]
      for holder_list in self.client.iterate(
        index=self.indices["event"],
        fields=[
          "concat('0x', substring(topics[3], 27, 40)) AS to",
          "argMin(concat('0x', substring(topics[2], 27, 40)), blockNumber) AS initial_sender"
        ],
        return_id=False,
        query="""
              WHERE concat('0x', substring(topics[2], 27, 40)) in({})
              AND address = '{}' 
              AND topics[1] = '{}'
              GROUP BY concat('0x', substring(topics[3], 27, 40))
            """.format(addresses_string, token, TRANSFER_EVENT_HEX)
      )
      for holder in holder_list
    ]
    token_receivers_df = pd.DataFrame(initiated_holders).groupby("initial_sender")["to"].nunique()
    total_token_receivers = self._get_total_token_receivers(token)
    return token_receivers_df / total_token_receivers

  def _get_features(self):
    all_addresses_stats = []
    for token_list in self._iterate_tokens():
      for token in token_list:
        for addresses in self._iterate_top_addresses(token["_source"]["address"]):
          addresses_stats = pd.DataFrame(
            [address["_source"]["address"] for address in addresses],
            columns=["address"]
          ).set_index("address")
          addresses_stats["token"] = token["_source"]["address"]
          addresses_stats["ethereum_senders"] = self._get_ethereum_senders(token, addresses)
          addresses_stats["events_per_transaction"] = self._get_events_per_transaction(token, addresses)
          addresses_stats["token_receivers"] = self._get_token_receivers(token, addresses)
          addresses_stats["initiated_holders"] = self._get_initiated_holders(token, addresses)
          all_addresses_stats.append(addresses_stats)

    return pd.concat(all_addresses_stats).fillna(0)

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