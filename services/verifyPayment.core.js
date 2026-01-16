export const verifyPaymentCore = async ({ supabase, rpcParams }) => {
  const { data, error } = await supabase.rpc(
    "verify_payment",
    rpcParams
  );

  if (error) {
    const isTimeout =
      error.code === "57014" ||
      error.message?.toLowerCase().includes("timeout") ||
      error.status === 504;

    if (isTimeout) {
      // ⛔ throw ONLY timeout
      throw error;
    }

    // ❌ non-timeout error returned normally
    return { data: null, error };
  }

  return { data, error: null };
};
