<?php

namespace DagaSmart\Edge\Http\Controllers;

use Illuminate\Http\Resources\Json\JsonResource;
use Illuminate\Http\JsonResponse;

class EdgeController extends AdminController
{
    public function index(): JsonResponse|JsonResource
    {
        $page = $this->basePage()->body('Edge Extension.');

        return $this->response()->success($page);
    }
}
