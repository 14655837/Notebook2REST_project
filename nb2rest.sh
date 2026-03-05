#!/bin/bash
# A simple script to invoke the notebook2rest Lambda function with a simulated API Gateway event.
# usage: ./nb2rest.sh POST /notebook/LiDAR_Vlab_tutorial.ipynb
#        ./nb2rest.sh GET /
#        ./nb2rest.sh POST /notebook/LiDAR_Vlab_tutorial.ipynb '{"param": "value"}'

METHOD=$1
API_PATH=$2
BODY=${3:-null}

AWS=$(command -v aws 2>/dev/null) || { echo "Error: aws CLI not found"; exit 1; }
$AWS sts get-caller-identity --no-cli-pager > /dev/null || { echo "Error: AWS identity check failed — verify credentials"; exit 1; }
PYTHON3=$(command -v python3 2>/dev/null || true)

format_json() {
#   if [ -n "$PYTHON3" ]; then
#     "$PYTHON3" -c '
# import sys, json
# data = sys.stdin.read()
# decoder = json.JSONDecoder()
# idx = 0
# while idx < len(data):
#     try:
#         obj, end = decoder.raw_decode(data, idx)
#         print(json.dumps(obj, indent=2))
#         idx = end
#         while idx < len(data) and data[idx] in " \t\n\r":
#             idx += 1
#     except Exception:
#         break
# '
#   else
    cat
  # fi
}

TMPFILE=$(mktemp)
trap "rm -f '$TMPFILE'" EXIT

METADATA=$($AWS lambda invoke \
  --function-name notebook2rest \
  --region eu-west-1 \
  --cli-binary-format raw-in-base64-out \
  --no-cli-pager \
  --payload "{
    \"version\": \"2.0\",
    \"routeKey\": \"$METHOD $API_PATH\",
    \"rawPath\": \"$API_PATH\",
    \"rawQueryString\": \"\",
    \"headers\": {\"content-type\": \"application/json\"},
    \"requestContext\": {
      \"http\": {
        \"method\": \"$METHOD\",
        \"path\": \"$API_PATH\",
        \"protocol\": \"HTTP/1.1\",
        \"sourceIp\": \"127.0.0.1\"
      },
      \"accountId\": \"123456789012\",
      \"apiId\": \"api-id\",
      \"stage\": \"\$default\",
      \"requestId\": \"id\"
    },
    \"body\": $BODY,
    \"isBase64Encoded\": false
  }" \
  "$TMPFILE" 2>/dev/null)

{ cat "$TMPFILE"; echo "$METADATA"; } | format_json