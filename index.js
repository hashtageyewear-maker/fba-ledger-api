// index.js  — FBA Inventory Ledger API

const express = require("express");
const SellingPartnerAPI = require("amazon-sp-api");
const axios = require("axios");
const { parse } = require("csv-parse/sync");
const zlib = require("zlib");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3000;

// India marketplace
const MARKETPLACE_ID = "A21TJRUUN4KGV";

function createSpClient() {
  return new SellingPartnerAPI({
    region: "eu", // India = EU
    refresh_token: process.env.REFRESH_TOKEN,
    credentials: {
      SELLING_PARTNER_APP_CLIENT_ID: process.env.LWA_CLIENT_ID,
      SELLING_PARTNER_APP_CLIENT_SECRET: process.env.LWA_CLIENT_SECRET,
      AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
      AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      AWS_SELLING_PARTNER_ROLE: process.env.AWS_SELLING_PARTNER_ROLE,
    },
    options: {
      auto_request_tokens: true,
      auto_request_throttled: true,
    },
  });
}

app.get("/", (req, res) => {
  res.send(
    'FBA Ledger API OK 👍  Use endpoint: GET /ledger?start=YYYY-MM-DD&end=YYYY-MM-DD'
  );
});

app.get("/ledger", async (req, res) => {
  try {
    const today = new Date();

    // ?start=2025-11-01&end=2025-11-30  (optional)
    const startDate = req.query.start
      ? new Date(req.query.start)
      : new Date(today.getFullYear(), today.getMonth(), 1);
    const endDate = req.query.end ? new Date(req.query.end) : today;

    const dataStartTime = startDate.toISOString();
    const dataEndTime = endDate.toISOString();

    console.log("Creating ledger report:", dataStartTime, "->", dataEndTime);

    const sp = createSpClient();

    // 1) Ledger report create karo
    const report = await sp.callAPI({
      operation: "createReport",
      endpoint: "reports",
      body: {
        reportType: "GET_LEDGER_DETAIL_VIEW_DATA",
        marketplaceIds: [MARKETPLACE_ID],
        dataStartTime,
        dataEndTime,
      },
    });

    const reportId = report.reportId;
    console.log("Report created:", reportId);

    // 2) Status poll karo jab tak DONE na ho jaye
    let details;
    for (let attempt = 0; attempt < 40; attempt++) {
      details = await sp.callAPI({
        operation: "getReport",
        endpoint: "reports",
        path: { reportId },
      });

      console.log(
        "Status attempt",
        attempt,
        "=>",
        details.processingStatus
      );

      if (details.processingStatus === "DONE") break;

      if (["CANCELLED", "FATAL"].includes(details.processingStatus)) {
        throw new Error("Report failed with status " + details.processingStatus);
      }

      // 15 sec wait
      await new Promise((r) => setTimeout(r, 15000));
    }

    if (!details || details.processingStatus !== "DONE") {
      return res.status(500).json({
        error: "Report not ready, try again. Status: " + details.processingStatus,
      });
    }

    // 3) Report document fetch karo
    const doc = await sp.callAPI({
      operation: "getReportDocument",
      endpoint: "reports",
      path: { reportDocumentId: details.reportDocumentId },
    });

    console.log("Downloading report file…");

    const response = await axios.get(doc.url, {
      responseType: "arraybuffer",
    });

    let buffer = Buffer.from(response.data);

    if (doc.compressionAlgorithm === "GZIP") {
      buffer = zlib.gunzipSync(buffer);
    }

    const csvText = buffer.toString("utf-8");

    // 4) CSV → JSON
    let records;
try {
  records = parse(csvText, {
    // Ledger report mostly TAB separated
    delimiter: "\t",          // <– important
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,       // invalid quote waali problem ignore
    relax_column_count: true, // agar kisi row me column zyada/कम ho to bhi allow
    bom: true,
  });
} catch (e) {
  console.error("CSV parse failed, first 300 chars:\n", csvText.slice(0, 300));
  throw e;
}


    console.log("Rows in ledger report:", records.length);

    res.json({
      reportId,
      from: dataStartTime,
      to: dataEndTime,
      rowCount: records.length,
      // NOTE: agar bahut zyada rows hon to slice kar sakte ho
      data: records,
    });
  } catch (err) {
    console.error("Ledger error:", err);
    res
      .status(500)
      .json({ error: err.message || "Error generating ledger report" });
  }
});

app.listen(PORT, () => {
  console.log(`FBA Ledger API running on http://localhost:${PORT}`);
});
   