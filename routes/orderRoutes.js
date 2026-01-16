import express from "express";
import { onOrderCreated } from "../controllers/orderController/orderController.js";

const router = express.Router();

// Supabase trigger will call this
router.post("/order-created", onOrderCreated);

export default router;
