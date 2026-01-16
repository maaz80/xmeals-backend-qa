// create route for payment
import { Router } from "express";
import { codOrderCreation, finalisePayment, initilisePayment } from "../controllers/paymentControllers/index.js";
import { verifyUser } from "../middlewares/PaymentMiddleware/verifyUser.js";

const router = Router();

router.post('/initiate-order',verifyUser, initilisePayment);
router.post('/finalize-order', verifyUser, finalisePayment);
router.post('/cod-order-creation', verifyUser, codOrderCreation);
export default router;