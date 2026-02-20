# ── Static External IP ────────────────────────────────────────────────────────
resource "google_compute_address" "neo4j_ip" {
  name    = "neo4j-static-ip"
  region  = var.region
  project = var.project_id
}

# ── Firewall: allow Neo4j ports ───────────────────────────────────────────────
resource "google_compute_firewall" "neo4j" {
  name    = "allow-neo4j"
  network = "default"
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["7474", "7687"]
  }

  source_ranges = ["153.92.90.3/32"]
  target_tags   = ["neo4j"]
}

# ── Firewall: allow SSH ───────────────────────────────────────────────────────
resource "google_compute_firewall" "ssh" {
  name    = "allow-ssh"
  network = "default"
  project = var.project_id

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = [
    "0.0.0.0/0",
    "35.235.240.0/20"
  ]
  target_tags = ["neo4j"]
}

# ── Secret Manager: Neo4j password ───────────────────────────────────────────
resource "google_secret_manager_secret" "neo4j_password" {
  secret_id = "neo4j-password-dev"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    env        = "dev"
    managed-by = "terraform"
  }
}

resource "google_secret_manager_secret_version" "neo4j_password_value" {
  secret      = google_secret_manager_secret.neo4j_password.id
  secret_data = var.neo4j_password
}

# ── GCE VM: Neo4j instance ────────────────────────────────────────────────────
resource "google_compute_instance" "neo4j" {
  name         = "neo4j-dev"
  machine_type = var.machine_type
  zone         = var.zone
  project      = var.project_id

  lifecycle {
    replace_triggered_by = [
      terraform_data.startup_script_hash
    ]
  }

  tags = ["neo4j"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = var.disk_size_gb
      type  = "pd-ssd"
    }
  }

  network_interface {
    network = "default"
    access_config {
      nat_ip = google_compute_address.neo4j_ip.address
    }
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    exec > /var/log/neo4j-install.log 2>&1
    echo "=== $(date) : Starting Neo4j installation ==="

    export DEBIAN_FRONTEND=noninteractive
    NEO4J_VERSION="5.26.0"
    NEO4J_HOME="/opt/neo4j"

    # ── Step 1: install Java 17 ───────────────────────────────────────────────
    apt-get update -y
    apt-get install -y openjdk-17-jre-headless curl
    java -version
    echo "=== $(date) : Java installed ==="

    # ── Step 2: download Neo4j tarball ────────────────────────────────────────
    curl -fsSL \
      "https://dist.neo4j.org/neo4j-community-$${NEO4J_VERSION}-unix.tar.gz" \
      -o /tmp/neo4j.tar.gz

    if file /tmp/neo4j.tar.gz | grep -q "HTML"; then
      echo "ERROR: neo4j tarball download failed"
      cat /tmp/neo4j.tar.gz
      exit 1
    fi

    ls -lh /tmp/neo4j.tar.gz
    echo "=== $(date) : Neo4j tarball downloaded ==="

    # ── Step 3: extract directly to /opt then rename ──────────────────────────
    # DO NOT mkdir /opt/neo4j first — mv needs the target to not exist
    tar -xzf /tmp/neo4j.tar.gz -C /opt/

    # verify extracted folder name
    ls /opt/ | grep neo4j
    mv /opt/neo4j-community-$${NEO4J_VERSION} $${NEO4J_HOME}

    # verify structure
    ls $${NEO4J_HOME}/bin/neo4j || { echo "ERROR: neo4j binary not found"; exit 1; }
    ls $${NEO4J_HOME}/conf/neo4j.conf || { echo "ERROR: neo4j.conf not found"; exit 1; }
    echo "=== $(date) : Neo4j extracted to $${NEO4J_HOME} ==="

    # ── Step 4: create neo4j user and set permissions ─────────────────────────
    useradd -r -M -s /bin/false neo4j || true
    chown -R neo4j:neo4j $${NEO4J_HOME}
    echo "=== $(date) : Permissions set ==="

    # ── Step 5: configure Neo4j ───────────────────────────────────────────────
    NEO4J_CONF="$${NEO4J_HOME}/conf/neo4j.conf"

    sed -i 's/#server.default_listen_address=0.0.0.0/server.default_listen_address=0.0.0.0/' \
      $${NEO4J_CONF}
    sed -i 's/#server.bolt.listen_address=:7687/server.bolt.listen_address=0.0.0.0:7687/' \
      $${NEO4J_CONF}
    sed -i 's/#server.http.listen_address=:7474/server.http.listen_address=0.0.0.0:7474/' \
      $${NEO4J_CONF}
    echo "=== $(date) : Neo4j configured ==="

    # ── Step 6: set initial password ─────────────────────────────────────────
    sudo -u neo4j $${NEO4J_HOME}/bin/neo4j-admin dbms set-initial-password "${var.neo4j_password}"
    echo "=== $(date) : Password set ==="

    # ── Step 7: create systemd service ───────────────────────────────────────
    cat > /etc/systemd/system/neo4j.service << 'SERVICE'
[Unit]
Description=Neo4j Graph Database
After=network.target
Wants=network.target

[Service]
Type=simple
User=neo4j
Group=neo4j
Environment="NEO4J_HOME=/opt/neo4j"
Environment="NEO4J_CONF=/opt/neo4j/conf"
ExecStart=/opt/neo4j/bin/neo4j console
Restart=on-failure
RestartSec=10
LimitNOFILE=60000
TimeoutStartSec=120

[Install]
WantedBy=multi-user.target
SERVICE

    echo "=== $(date) : Systemd service created ==="

    # ── Step 8: enable and start ──────────────────────────────────────────────
    systemctl daemon-reload
    systemctl enable neo4j
    systemctl start neo4j
    echo "=== $(date) : Neo4j service started ==="

    # ── Step 9: wait and verify ───────────────────────────────────────────────
    echo "=== $(date) : Waiting 40s for Neo4j to fully start ==="
    sleep 40

    systemctl is-active neo4j \
      && echo "=== $(date) : Neo4j is RUNNING ✅ ===" \
      || { echo "=== $(date) : Neo4j FAILED ❌ ==="; journalctl -u neo4j -n 30 --no-pager; }
  EOT


  service_account {
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  labels = {
    env        = "dev"
    managed-by = "terraform"
    app        = "neo4j"
  }
}

# ── triggers VM replacement when script or key variables change ───────────────
resource "terraform_data" "startup_script_hash" {
  input = sha256(<<-EOT
    neo4j-install-v8
    ${var.neo4j_password}
    ${var.machine_type}
    ${var.disk_size_gb}
  EOT
  )
}