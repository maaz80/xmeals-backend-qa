import express from "express";
import { razorpayWebhook } from "../controllers/razorpayWebhook/razorpayWebhookController.js";

const router = express.Router();

// RAW BODY PARSER — IMPORTANT
router.post("/webhook", express.raw({ type: "application/json" }), razorpayWebhook);

export default router;
