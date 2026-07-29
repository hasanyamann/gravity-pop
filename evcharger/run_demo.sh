#!/usr/bin/env bash
# Sunucuyu ve sanal sarj cihazini birlikte baslatir.
#
#   ./run_demo.sh            gercek zamanli (onerilen)
#   ./run_demo.sh 60         zamani 60 kat hizlandirir
#
# Not: hizlandirma yalnizca test icindir. Sunucu sureyi gercek saatle
# olctugu halde enerji hizlandirilmis zamanla biriktigi icin "Ortalama
# kW" ve "Sure" alanlari sasirtici gorunur. Hizli ama tutarli bir demo
# istiyorsan bunun yerine kucuk batarya kullan: --battery 3
#
# Ardindan tarayicidan http://localhost:8080 adresini ac.

set -euo pipefail
cd "$(dirname "$0")"

SPEED="${1:-1}"
pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Merkezi sistem baslatiliyor..."
python3 central_system.py &
pids+=($!)

# Sunucunun OCPP portunu acmasini bekle; sabit bir uyku yerine porta
# bakiyoruz ki yavas makinelerde de calissin.
for _ in $(seq 1 50); do
  if python3 -c "import socket,sys; s=socket.socket(); sys.exit(s.connect_ex(('127.0.0.1',9000)))" 2>/dev/null; then
    break
  fi
  sleep 0.2
done

echo "Sanal sarj cihazi baslatiliyor (hiz carpani: ${SPEED})..."
python3 simulator.py --speed "$SPEED" &
pids+=($!)

echo
echo "  Telefon arayuzu:  http://localhost:8080"
echo "  Durdurmak icin:   Ctrl+C"
echo

wait
