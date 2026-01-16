import express from "express";
import { whatsappWebhook, verifyWebhook } from "../controllers/whatsappController/whatsappController.js";

const router = express.Router();

// WhatsApp webhook verification (GET)
router.get("/whatsapp", verifyWebhook);

// WhatsApp sends messages here (POST)
router.post("/whatsapp", whatsappWebhook);

export default router;
